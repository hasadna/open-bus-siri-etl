import datetime
import multiprocessing
from collections import defaultdict

from . import process_snapshot


def worker(input, output):
    for snapshot_id_from, snapshot_id_to in iter(input.get, 'STOP'):
        try:
            stats = process_snapshot.process_snapshots(
                snapshot_id_from, snapshot_id_to,
                force_reload=False, download=True, only_missing=True
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


def main(processes=4, batch_minutes=60, max_history_minutes=60*24*365):
    processes = int(processes)
    batch_minutes = int(batch_minutes)
    max_history_minutes = int(max_history_minutes)
    dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(**process_snapshot.DEFAULT_SNAPSHOTS_TIMEDELTA) + datetime.timedelta(minutes=3)
    print("Starting {} processes to process old missing snapshots from {}".format(processes, dt))
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()
    num_tasks = 0
    while dt > datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=max_history_minutes):
        snapshot_id_from = dt.strftime('%Y/%m/%d/%H/%M')
        dt = dt - datetime.timedelta(minutes=batch_minutes)
        snapshot_id_to = dt.strftime('%Y/%m/%d/%H/%M')
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
