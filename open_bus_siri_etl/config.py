import os
from open_bus_siri_requester.config import OPEN_BUS_SIRI_STORAGE_ROOTPATH


DEBUG = os.environ.get('DEBUG') == 'yes'
OPEN_BUS_SIRI_ETL_ROOTPATH = os.path.join(OPEN_BUS_SIRI_STORAGE_ROOTPATH, 'etl')

# this configuration should be hard-coded so it's the same
# on both airflow manual runs and on the new snapshots daemon
OPEN_BUS_SIRI_ETL_USE_OBJECTSMAKER_CACHE = True
