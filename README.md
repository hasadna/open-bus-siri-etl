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

Upgrade pip

```
venv/bin/pip install --upgrade pip
```

You should have a clone of the following repositories in sibling directories:

* `../open-bus-siri-requester`: https://github.com/hasadna/open-bus-siri-requester
* `../open-bus-stride-db`: https://github.com/hasadna/open-bus-stride-db

Install dev requirements (this installs above repositories as well as this repository as editable for development):

```
pip install -r requirements-dev.txt
```

Create a `.env` file and set the following in the file:

Get the values for the remote url from another project member:

```
export REMOTE_URL_HTTPAUTH=username:password
```

The sql alchemy url should be as follows (it's only used locally):

```
export SQLALCHEMY_URL=postgresql://postgres:123456@localhost
```

Enable debug for local development:

```
export DEBUG=yes
```

## Use

Go to open-bus-stride-db repo and follow the README to start a local DB and update to latest migration

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

Start a stride DB for testing and update to latest migration:

```
docker rm -f stride-db;
sudo rm -rf .data/tests-db &&\
docker run --rm --name stride-db -e POSTGRES_PASSWORD=123456 -p 5432:5432 -v `pwd`/.data/tests-db:/var/lib/postgresql/data -d postgres:13 &&\
sleep 2 &&\
(cd ../open-bus-stride-db && alembic upgrade head)
```

Run tests

```
pytest
```