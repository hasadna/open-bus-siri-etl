import datetime
import multiprocessing
from collections import defaultdict

from . import process_snapshot, common


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


def main(from_date=None, to_date=None, processes=4, batch_minutes=60):
    processes = int(processes)
    batch_minutes = int(batch_minutes)
    from_date = common.parse_date_str(from_date)
    to_date = common.parse_date_str(to_date, num_days=5)
    if to_date > from_date:
        dt = to_date
        min_dt = from_date
    else:
        dt = from_date
        min_dt = to_date
    dt = datetime.datetime.combine(dt, datetime.time(23, 59))
    min_dt = datetime.datetime.combine(min_dt, datetime.time(0, 0))
    print("Starting {} processes to process old missing snapshots from {} to {}".format(processes, dt, min_dt))
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()
    num_tasks = 0
    while dt >= min_dt:
        snapshot_id_from = dt.strftime('%Y/%m/%d/%H/%M')
        dt = dt - datetime.timedelta(minutes=batch_minutes)
        snapshot_id_to = (min_dt if dt < min_dt else dt).strftime('%Y/%m/%d/%H/%M')
        task_queue.put((snapshot_id_from, snapshot_id_to))
        num_tasks += 1
    print("{} tasks in queue, starting processing".format(num_tasks))
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
