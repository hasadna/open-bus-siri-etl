import os
from open_bus_siri_requester.config import OPEN_BUS_SIRI_STORAGE_ROOTPATH


DEBUG = os.environ.get('DEBUG') == 'yes'
OPEN_BUS_SIRI_ETL_ROOTPATH = os.path.join(OPEN_BUS_SIRI_STORAGE_ROOTPATH, 'etl')
SNAPSHOT_DOWNLOAD_REMOTE_URL = 'https://openbus-stride-public.s3.eu-west-1.amazonaws.com/stride-siri-requester'
