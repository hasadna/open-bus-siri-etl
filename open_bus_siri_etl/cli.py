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
@click.option('--force-reload', is_flag=True)
def process_snapshot(snapshot_id, force_reload):
    open_bus_siri_etl.process_snapshot.process_snapshot(snapshot_id, force_reload)


@main.command()
@click.option('--limit')
def process_new_snapshots(limit):
    open_bus_siri_etl.process_snapshot.process_new_snapshots(limit)


@main.command()
def start_process_new_snapshots_daemon():
    open_bus_siri_etl.process_snapshot.start_process_new_snapshots_daemon()
