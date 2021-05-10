import pytest

from open_bus_stride_db.db import Session


@pytest.fixture()
def session():
    with Session() as session:
        yield session
