import os
import datetime

import requests

import open_bus_siri_requester.config

from . import common


REMOTE_URL = 'https://openbus-stride-public.s3.eu-west-1.amazonaws.com/stride-siri-requester'


def download_latest_snapshots():
    now = common.now()
    for i in reversed(range(1, 120)):
        snapshot_id = (now - datetime.timedelta(minutes=i)).strftime('%Y/%m/%d/%H/%M')
        download_snapshot(snapshot_id)


def download_snapshot(snapshot_id):
    filename = '{}.br'.format(snapshot_id)
    url = '{}/{}'.format(REMOTE_URL, filename)
    filepath = os.path.join(open_bus_siri_requester.config.OPEN_BUS_SIRI_STORAGE_ROOTPATH, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        try:
            print("Downloading {} -> {}".format(url, filepath))
            f.write(requests.get(url).content)
        except:
            print("Failed to download {}".format(filename))
