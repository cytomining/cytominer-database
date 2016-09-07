
import pytest
import subprocess

def pytest_addoption(parser):
    parser.addoption("--dataset", action="store", default="htqc", help="dataset to test")

    parser.addoption("--engine", action="store", default="postgres", help="database engine", choices=["sqlite", "postgres"])


def pytest_generate_tests(metafunc):
    if "session" in metafunc.fixturenames:
        assert metafunc.config.option.engine in ["postgres", "sqlite"]

        metafunc.parametrize("session", [metafunc.config.option.engine], indirect=True)

    if "dataset" in metafunc.fixturenames:
        assert metafunc.config.option.dataset in ["htqc", "cellpainting"]

        metafunc.parametrize("dataset", [metafunc.config.option.dataset], indirect=True)


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
        raise ValueError("No such engine: {}".format(request.param))

    engine = sqlalchemy.create_engine(connection_string)

    session_generator = sqlalchemy.orm.sessionmaker(bind=engine)

    return session_generator()


@pytest.fixture
def dataset(request):
    if request.param == "htqc":
        return  {
                "data_dir" : "test/data_a",
                "row_counts" :
                    {
                        "n_plates" : 1,
                        "n_channels" : 3,
                        "n_channels_raddist" : 3,
                        "n_patterns" : 3,
                        "n_wells" : 4,
                        "n_images" : 8,
                        "n_objects" : 40,
                        "n_bins_raddist" : 4,
                        "n_scales_texture" : 3,
                        "n_scales_neighborhood" : 2,
                        "n_moments_coefs" : 30,
                        "n_correlation_pairs" : 5
                    },
                "ingest" :
                    {
                        "image_nrows" : 8,
                        "image_ncols" : 229,
                        "Cells_nrows" : 40,
                        "Cells_ncols" : 294,
                        "Cytoplasm_nrows" : 40,
                        "Cytoplasm_ncols" : 279,
                        "Nuclei_nrows" : 40,
                        "Nuclei_ncols" : 287
                    },
                "munge" : True
                }

    elif request.param == "cellpainting":
        return  {
                "data_dir" : "test/data_b",
                "row_counts" :
                    {
                        "n_plates" : 1,
                        "n_channels" : 5,
                        "n_channels_raddist" : 4,
                        "n_patterns" : 3,
                        "n_wells" : 2,
                        "n_images" : 4,
                        "n_objects" : 40,
                        "n_bins_raddist" : 4,
                        "n_scales_texture" : 3,
                        "n_scales_neighborhood" : 2,
                        "n_moments_coefs" : 30,
                        "n_correlation_pairs" : 10
                    },
                "ingest" :
                    {
                        "Image_nrows" : 4,
                        "Image_ncols" : 6,
                        "Cells_nrows" : 40,
                        "Cells_ncols" : 586,
                        "Cytoplasm_nrows" : 40,
                        "Cytoplasm_ncols" : 572,
                        "Nuclei_nrows" : 40,
                        "Nuclei_ncols" : 595
                    },
                "munge" : False
                }        

    else:
        raise ValueError("No such dataset: {}".format(request.param))
