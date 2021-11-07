import os
from open_bus_siri_requester.config import OPEN_BUS_SIRI_STORAGE_ROOTPATH


DEBUG = os.environ.get('DEBUG') == 'yes'
OPEN_BUS_SIRI_ETL_ROOTPATH = os.path.join(OPEN_BUS_SIRI_STORAGE_ROOTPATH, 'etl')
