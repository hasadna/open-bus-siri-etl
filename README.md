# Open Bus SIRI ETL

ETL processing of SIRI real-time snapshots


## Install

Install Brotli for compression

```
sudo apt-get install brotli
```

Create virtualenv (Python 3.8)

```
python3.8 -m venv venv
```

Install open-bus-siri-requester dependencies and package, assuming it is cloned in a sibling directory:

```
venv/bin/python -m pip install -r ../open-bus-siri-requester/requirements.txt &&\
venv/bin/python -m pip install -e ../open-bus-siri-requester
```

Install open-bus-stride-db dependencies and package, assuming it is cloned in a sibling directory:

```
venv/bin/python -m pip install -r ../open-bus-stride-db/requirements.txt &&\
venv/bin/python -m pip install -e ../open-bus-stride-db
```

Install open-bus-siri-etl dependencies and package 

```
venv/bin/python -m pip install -r requirements.txt &&\
venv/bin/python -m pip install -e .
```

Create a `.env`:

```
export REMOTE_URL_HTTPAUTH=username:password
export SQLALCHEMY_URL=postgresql://postgres:123456@localhost
```

## Use

Start a stride DB:

```
docker run --name stride-db -e POSTGRES_PASSWORD=123456 -p 5432:5432 -v `pwd`/.data/db:/var/lib/postgresql/data -d postgres:13
```

Activate the virtualenv and source the .env file

```
. venv/bin/activate
source .env
```

Download latest snapshots

```
open-bus-siri-etl download-latest-snapshots
```

List some snapshots

```
open-bus-siri-requester storage-list
```

Process a snapshot

```
open-bus-siri-etl process-snapshot SNAPSHOT_ID
```

## Tests

Install tests requirements

```
pip install -r tests/requirements.txt
```

Start a stride DB for testing:

```
sudo rm -rf .data/tests-db &&\
docker run --rm --name stride-db -e POSTGRES_PASSWORD=123456 -p 5432:5432 -v `pwd`/.data/tests-db:/var/lib/postgresql/data -d postgres:13 &&\
sleep 2 &&\
(cd ../open-bus-stride-db && alembic upgrade head)
```

Run tests

```
pytest
```