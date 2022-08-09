import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_siri_requester import storage

from . import common


def update_snapshot_ids(session, snapshot_ids_batch, stats):
    existing_snapshot_ids = set()
    for row in session.execute(dedent(f"""
        select snapshot_id from siri_snapshot
        where snapshot_id in ({','.join([f"'{snapshot_id}'" for snapshot_id in snapshot_ids_batch])})
    """)):
        existing_snapshot_ids.add(row.snapshot_id)
    insert_snapshot_ids = snapshot_ids_batch - existing_snapshot_ids
    if len(insert_snapshot_ids) > 0:
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


def main(session=None):
    stats = defaultdict(int)
    with get_session(session) as session:
        row = session.execute(dedent("""select date from gtfs_data order by date limit 1""")).one_or_none()
        if row is None:
            first_gtfs_date = None
        else:
            first_gtfs_date = row.date
        snapshot_ids_batch = set()
        for snapshot_id in storage.list():
            stats['total snapshot ids'] += 1
            snapshot_datetime = datetime.datetime.strptime(snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z')
            if first_gtfs_date is None or snapshot_datetime.date() >= first_gtfs_date:
                stats['valid snapshot ids for update'] += 1
                snapshot_ids_batch.add(snapshot_id)
                if len(snapshot_ids_batch) >= 1000:
                    update_snapshot_ids(session, snapshot_ids_batch, stats)
                    snapshot_ids_batch.clear()
        if len(snapshot_ids_batch) > 0:
            update_snapshot_ids(session, snapshot_ids_batch, stats)
    pprint(dict(stats))
    print('OK')
