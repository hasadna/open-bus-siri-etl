# Open Bus SIRI ETL

ETL processing of SIRI real-time snapshots

See [our contributing docs](https://github.com/hasadna/open-bus-pipelines/blob/main/CONTRIBUTING.md) if you want to suggest changes to this repository.

## Development using the Docker Compose environment

This is the easiest option to start development, follow these instructions: https://github.com/hasadna/open-bus-pipelines/blob/main/README.md#siri-etl

For local development, see the additional functionality section: `Develop siri-etl from a local clone`

## Development using local Python interpreter

It's much easier to use the Docker Compose environment, but the following can be
refferd to for more details regarding the internal processes and for development
using your local Python interpreter. 

### Install

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

The sql alchemy url should be as follows (it's only used locally):

```
export SQLALCHEMY_URL=postgresql://postgres:123456@localhost
```

Enable debug for local development:

```
export DEBUG=yes
```

### Use

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

### Tests

Install tests requirements

```
pip install -r tests/requirements.txt
```

Start a fresh stride DB for testing, run the following from open-bus-pipelines repo:

```
docker-compose down; docker volume rm open-bus-pipelines_stride-db; docker-compose up -d stride-db-init
```

Run tests

```
pytest
```
