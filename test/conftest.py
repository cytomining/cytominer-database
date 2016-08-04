
import pytest
import subprocess
import perturbation.base
import sqlalchemy
import sqlalchemy.orm

def pytest_addoption(parser):
    parser.addoption("--dataset", action="store", default="htqc", help="dataset to test")

    parser.addoption("--engine", action="store", default="postgres", help="database engine", choices=["sqlite", "postgres"])


@pytest.fixture
def selected_dataset(request):
    return request.config.getoption("--dataset")



def pytest_generate_tests(metafunc):
    if "session" in metafunc.fixturenames:
        assert metafunc.config.option.engine in ["postgres", "sqlite"]

        metafunc.parametrize("session", [metafunc.config.option.engine], indirect=True)


@pytest.fixture
def session(request):
    if request.param == "postgres":
        cmd = 'PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "DROP DATABASE IF EXISTS testdb"'

        subprocess.check_output(cmd, shell=True)

        cmd = 'PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "CREATE DATABASE testdb"'

        subprocess.check_output(cmd, shell=True)

        connection_string = "postgresql://postgres:password@localhost:3210/testdb"

    elif request.param == "sqlite":
        connection_string = "sqlite:////tmp/example.sqlite"

    else:
        raise ValueError("invalid internal test config")

    engine = sqlalchemy.create_engine(connection_string)

    session_generator = sqlalchemy.orm.sessionmaker(bind=engine)

    return session_generator()
