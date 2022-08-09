import os
import json
import datetime
import subprocess
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_siri_requester import storage

from . import common, config


def s3api_list_objects(prefix, max_keys):
    return json.loads(subprocess.check_output(
        [
            'aws', '--output', 'json', 's3api', 'list-objects',
            '--bucket', config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME,
            '--prefix', os.path.join(config.OPEN_BUS_STRIDE_PUBLIC_S3_SIRI_REQUESTER_OBJECT_PREFIX, prefix),
            '--max-keys', str(max_keys),
        ],
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY,
        }
    )).get('Contents', [])


def s3api_prefix_has_files(prefix):
    return len(s3api_list_objects(prefix, 1)) > 0


def iterate_remote_siri_snapshots(start_date):
    for year in range(start_date.year, datetime.date.today().year+1):
        if s3api_prefix_has_files(f'{year}/'):
            for month in range(start_date.month if year == start_date.year else 1, 13):
                if s3api_prefix_has_files(f'{year}/{month:02d}/'):
                    for day in range(start_date.day if (year == start_date.year and month == start_date.month) else 1, 32):
                        if s3api_prefix_has_files(f'{year}/{month:02d}/{day:02d}/'):
                            for hour in range(24):
                                for obj in s3api_list_objects(f'{year}/{month:02d}/{day:02d}/{hour:02d}/', 1000):
                                    yield '/'.join(obj['Key'].split('/')[1:]).replace('.br', '')


def update_snapshot_ids(session, snapshot_ids_batch, stats, verbose=False):
    existing_snapshot_ids = set()
    for row in session.execute(dedent(f"""
        select snapshot_id from siri_snapshot
        where snapshot_id in ({','.join([f"'{snapshot_id}'" for snapshot_id in snapshot_ids_batch])})
    """)):
        existing_snapshot_ids.add(row.snapshot_id)
    insert_snapshot_ids = snapshot_ids_batch - existing_snapshot_ids
    if len(insert_snapshot_ids) > 0:
        if verbose:
            print(f"Inserting {len(insert_snapshot_ids)} snapshot ids...")
        strnow = common.now().strftime('%Y-%m-%d %H:%M:%S')
        values = [
            f"('{snapshot_id}', '{strnow}', 'pending')"
            for snapshot_id in insert_snapshot_ids
        ]
        session.execute(dedent(f"""
            insert into siri_snapshot (snapshot_id, etl_pending_time, etl_status)
            values {','.join(values)}
        """))
        session.commit()
        stats['inserted snapshot ids'] += len(insert_snapshot_ids)


def iterate_snapshot_ids(with_remote_snapshots=False, start_date=None, verbose=False):
    if verbose:
        print("Iterating local snapshots...")
    yield from storage.list()
    if with_remote_snapshots:
        assert start_date
        if verbose:
            print("Iterating remote snapshots...")
        for i, snapshot_id in enumerate(iterate_remote_siri_snapshots(start_date)):
            yield snapshot_id
            if i > 0 and i % 1000 == 0:
                print(f"{i+1} remote snapshots iterated (last snapshot id: {snapshot_id})")


def main(session=None, with_remote_snapshots=False, verbose=False):
    stats = defaultdict(int)
    with get_session(session) as session:
        row = session.execute(dedent("""select date from gtfs_data order by date limit 1""")).one_or_none()
        if row is None:
            first_gtfs_date = None
        else:
            first_gtfs_date = row.date
        snapshot_ids_batch = set()
        for snapshot_id in iterate_snapshot_ids(with_remote_snapshots, first_gtfs_date, verbose):
            stats['total snapshot ids'] += 1
            snapshot_datetime = datetime.datetime.strptime(snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z')
            if first_gtfs_date is None or snapshot_datetime.date() >= first_gtfs_date:
                stats['valid snapshot ids for update'] += 1
                snapshot_ids_batch.add(snapshot_id)
                if len(snapshot_ids_batch) >= 1000:
                    update_snapshot_ids(session, snapshot_ids_batch, stats, verbose)
                    snapshot_ids_batch.clear()
        if len(snapshot_ids_batch) > 0:
            update_snapshot_ids(session, snapshot_ids_batch, stats, verbose)
    pprint(dict(stats))
    print('OK')
