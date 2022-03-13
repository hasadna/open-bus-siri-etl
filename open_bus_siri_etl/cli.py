import datetime

import click

import open_bus_siri_etl.process_snapshot
from open_bus_siri_etl import local_development_helpers


@click.group(context_settings={'max_content_width': 200})
def main():
    """Open Bus SIRI ETL"""
    pass


@main.command()
def download_latest_snapshots():
    """Download snapshots of last 2 hours from remote storage to local storage"""
    local_development_helpers.download_latest_snapshots()


@main.command()
@click.argument('SNAPSHOT_ID')
def download_snapshot(snapshot_id):
    """Download a specific snapshot from remote storage to local storage"""
    local_development_helpers.download_snapshot(snapshot_id)


@main.command()
@click.argument('SNAPSHOT_ID')
@click.option('--force-reload', is_flag=True)
@click.option('--download', is_flag=True)
def process_snapshot(**kwargs):
    open_bus_siri_etl.process_snapshot.process_snapshot(**kwargs)


@main.command()
@click.argument('SNAPSHOT_ID_FROM')
@click.argument('SNAPSHOT_ID_TO')
@click.option('--force-reload', is_flag=True)
@click.option('--download', is_flag=True)
def process_snapshots(**kwargs):
    open_bus_siri_etl.process_snapshot.process_snapshots(**kwargs)


@main.command()
@click.option('--limit')
def process_new_snapshots(limit):
    open_bus_siri_etl.process_snapshot.process_new_snapshots(limit)


@main.command()
def start_process_new_snapshots_daemon():
    open_bus_siri_etl.process_snapshot.start_process_new_snapshots_daemon()


if __name__ == '__main__':
    main()
