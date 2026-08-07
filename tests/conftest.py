import pytest

from lakehouse_bronze_matriculas import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(app_name="tests")
    yield session
    session.stop()
