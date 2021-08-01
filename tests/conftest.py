import pytest

from open_bus_stride_db.db import get_session


@pytest.fixture()
def session():
    with get_session() as session:
        yield session
