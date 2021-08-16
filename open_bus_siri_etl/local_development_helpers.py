import os
import datetime

import pytz
import requests

import open_bus_siri_requester.config


REMOTE_URL = 'https://open-bus-siri-requester.hasadna.org.il'
REMOTE_URL_HTTPAUTH = os.environ.get('REMOTE_URL_HTTPAUTH')


def download_latest_snapshots():
    assert REMOTE_URL_HTTPAUTH, 'missing REMOTE_URL_HTTPAUTH env var, it should be in the format username:password'
    now = datetime.datetime.now(pytz.UTC)
    for i in reversed(range(1, 120)):
        snapshot_id = (now - datetime.timedelta(minutes=i)).strftime('%Y/%m/%d/%H/%M')
        download_snapshot(snapshot_id)


def download_snapshot(snapshot_id):
    assert REMOTE_URL_HTTPAUTH, 'missing REMOTE_URL_HTTPAUTH env var, it should be in the format username:password'
    filename = '{}.br'.format(snapshot_id)
    url = '{}/{}'.format(REMOTE_URL, filename)
    filepath = os.path.join(open_bus_siri_requester.config.OPEN_BUS_SIRI_STORAGE_ROOTPATH, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        try:
            print("Downloading {} -> {}".format(url, filepath))
            f.write(requests.get(url, auth=tuple(REMOTE_URL_HTTPAUTH.split(':'))).content)
        except:
            print("Failed to download {}".format(filename))
