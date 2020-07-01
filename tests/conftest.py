import pytest

def pytest_addoption(parser): # where is this used?
    parser.addoption("--dataset", action="store", help="dataset to test")


def pytest_generate_tests(metafunc): # where is this used?
    if "dataset" in metafunc.fixturenames:
        if metafunc.config.option.dataset is None:
            metafunc.parametrize("dataset", ["htqc", "cellpainting", "qc"], indirect=True)
        else:
            assert metafunc.config.option.dataset in ["htqc", "cellpainting", "qc"]

            metafunc.parametrize("dataset", [metafunc.config.option.dataset], indirect=True)


@pytest.fixture
def cellpainting():
    """
    Return configuration for a Cell Painting dataset.
    - 3 compartments CSVs: Cells, Cytoplasm, Nuclei
    - 1 image CSV
    - No object.csv and therefore no munging
    """
    return {
        "config": "config_Parquet.ini",
        "data_dir": "tests/data_b",
        "image_csv": "Image.csv",
        "ingest": [
            {
                "ncols": 586,
                "nrows": 40,
                "table": "Cells"
            },
            {
                "ncols": 572,
                "nrows": 40,
                "table": "Cytoplasm"
            },
            {
                "ncols": 6,
                "nrows": 4,
                "table": "image"
            },
            {
                "ncols": 595,
                "nrows": 40,
                "table": "Nuclei"
            }
        ],
        "munge": False
    }

@pytest.fixture
def htqc():
    """
    Return configuration for a 3-channel image-based profiling dataset.
    - 1 object CSV that comprises Cells, Cytoplasm, Nuclei
    - 1 image CSV
    - munging required
    """
    return {
        "config": "config_Parquet.ini",
        "data_dir": "tests/data_a",
        "munged_dir": "tests/data_a_munged",
        "image_csv": "image.csv",
        "ingest": [
            {
                "ncols": 294,
                "nrows": 40,
                "table": "Cells"
            },
            {
                "ncols": 279,
                "nrows": 40,
                "table": "Cytoplasm"
            },
            {
                "ncols": 229,
                "nrows": 8,
                "table": "image"
            },
            {
                "ncols": 287,
                "nrows": 40,
                "table": "Nuclei"
            }
        ],
        "munge": True
    }


@pytest.fixture
def qc():
    """
    Return configuration for a QC dataset (only image table, no objects).
    - 1 image CSV
    - No object.csv and therefore no munging
    """
    return {
        "config": None,
        "data_dir": "tests/data_c",
        "image_csv": "Image.csv",
        "ingest": [
            {
                "nrows": 8,
                "ncols": 229,
                "table": "Image"
            }
        ],
        "munge": True
    }


@pytest.fixture
def dataset(request):
    # Note that calling fixtures directly is deprecated:
    # https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
    # Instead, use the request fixture to dynamically run the named fixture function:
    # https://docs.pytest.org/en/latest/reference.html#_pytest.fixtures.FixtureRequest.getfixturevalue
    dataset_param = request.param

    if dataset_param == "htqc":
        return request.getfixturevalue('htqc')

    if dataset_param == "cellpainting":
        return request.getfixturevalue('cellpainting')

    if dataset_param == "qc":
        return request.getfixturevalue('qc')

    raise ValueError("No such dataset: {}".format(request.param))

@pytest.fixture
def engine_choice(request):
    # Note that calling fixtures directly is deprecated:
    # https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
    # Instead, use the request fixture to dynamically run the named fixture function:
    # https://docs.pytest.org/en/latest/reference.html#_pytest.fixtures.FixtureRequest.getfixturevalue
    engine_param = request.param

    if engine_param == "Parquet":
        return request.getfixturevalue('Parquet')

    if engine_param == "SQLite":
        return request.getfixturevalue('SQLite')


    raise ValueError("No such dataset: {}".format(request.param))
