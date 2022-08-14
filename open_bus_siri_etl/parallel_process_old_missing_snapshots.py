import datetime
import multiprocessing
from textwrap import dedent
from collections import defaultdict

from open_bus_stride_db.db import get_session

from . import process_snapshot


def worker(input, output):
    for snapshot_id_from, snapshot_id_to in iter(input.get, 'STOP'):
        try:
            stats = process_snapshot.process_snapshots(
                snapshot_id_from, snapshot_id_to,
                force_reload=True, download=True, only_missing=True
            )
            result = {
                'snapshot_id_from': snapshot_id_from,
                'snapshot_id_to': snapshot_id_to,
                'stats': dict(stats)
            }
        except Exception as e:
            result = {
                'snapshot_id_from': snapshot_id_from,
                'snapshot_id_to': snapshot_id_to,
                'error': str(e)
            }
        output.put(result)


def iterate_pending_snapshots():
    with get_session() as session:
        for row in session.execute(dedent("""
            select snapshot_id from siri_snapshot 
            where etl_status = 'pending' 
            order by snapshot_id desc 
        """)):
            yield row.snapshot_id


def iterate_consecutive_snapshot_ids():
    snapshot_id_from, dt_from = None, None
    snapshot_id_to, dt_to = None, None
    for snapshot_id in iterate_pending_snapshots():
        dt = datetime.datetime.strptime(snapshot_id, '%Y/%m/%d/%H/%M')
        if snapshot_id_from is None:
            # first snapshot
            snapshot_id_from, dt_from = snapshot_id, dt
        else:
            if snapshot_id_to is None:
                prev_snapshot_id, prev_dt = snapshot_id_from, dt_from
            else:
                prev_snapshot_id, prev_dt = snapshot_id_to, dt_to
            if dt - prev_dt == datetime.timedelta(minutes=1):
                # snapshot is consecutive to the previous snapshot in the batch, add it to the batch
                snapshot_id_to, dt_to = snapshot_id, dt
            else:
                # snapshot is not consecutive - yield the batch
                yield snapshot_id_from, prev_snapshot_id
                # this snapshot is now first snapshot in a new batch
                snapshot_id_from, dt_from = snapshot_id, dt
                snapshot_id_to, dt_to = None, None
    if snapshot_id_from is not None:
        # yield the last batch
        if snapshot_id_to is None:
            yield snapshot_id_from, snapshot_id_from
        else:
            yield snapshot_id_from, snapshot_id_to


def iterate_snapshot_id_batches(batch_minutes):
    for snapshot_id_from, snapshot_id_to in iterate_consecutive_snapshot_ids():
        dt_from = datetime.datetime.strptime(snapshot_id_from, '%Y/%m/%d/%H/%M')
        dt_to = datetime.datetime.strptime(snapshot_id_to, '%Y/%m/%d/%H/%M')
        if dt_to - dt_from <= datetime.timedelta(minutes=batch_minutes):
            yield snapshot_id_from, snapshot_id_to
        else:
            batch = []
            dt = dt_from
            while dt <= dt_to:
                batch.append(dt)
                if len(batch) == batch_minutes:
                    yield batch[0].strftime('%Y/%m/%d/%H/%M'), batch[-1].strftime('%Y/%m/%d/%H/%M')
                    batch = []
                dt += datetime.timedelta(minutes=1)
            if len(batch) > 0:
                yield batch[0].strftime('%Y/%m/%d/%H/%M'), batch[-1].strftime('%Y/%m/%d/%H/%M')


def main(processes=4, batch_minutes=60):
    processes = int(processes)
    batch_minutes = int(batch_minutes)
    print(f"Starting {processes} processes to process old missing snapshots in batches of {batch_minutes} snapshots")
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()
    num_tasks = 0
    for snapshot_id_from, snapshot_id_to in iterate_snapshot_id_batches(batch_minutes):
        task_queue.put((snapshot_id_from, snapshot_id_to))
        num_tasks += 1
    print(f"{num_tasks} tasks in queue, starting processing")
    for _ in range(processes):
        multiprocessing.Process(target=worker, args=(task_queue, done_queue)).start()
    print("Waiting for tasks to complete...")
    stats = defaultdict(int)
    for _ in range(num_tasks):
        result = done_queue.get()
        if result.get('error'):
            stats['num_errors'] += 1
            print(f'task failed - {result["snapshot_id_from"]} - {result["snapshot_id_to"]} - {result["error"]}')
        else:
            stats['num_success'] += 1
            stats['processed snapshots'] += result['stats'].get('processed snapshots', 0)
            stats['missing snapshots'] += result['stats'].get('missing snapshots', 0)
            stats['existing snapshots'] += result['stats'].get('existing snapshots', 0)
        print(dict(stats))
    for i in range(processes):
        task_queue.put('STOP')
