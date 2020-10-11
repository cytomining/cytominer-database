import pytest


def pytest_addoption(parser):  #  where is this used?
    parser.addoption("--dataset", action="store", help="dataset to test")


def pytest_generate_tests(metafunc):  #  where is this used?
    if "dataset" in metafunc.fixturenames:
        if metafunc.config.option.dataset is None:
            metafunc.parametrize(
                "dataset", ["htqc", "cellpainting", "qc"], indirect=True
            )
        else:
            assert metafunc.config.option.dataset in ["htqc", "cellpainting", "qc"]

            metafunc.parametrize(
                "dataset", [metafunc.config.option.dataset], indirect=True
            )


@pytest.fixture
def cellpainting():
    """
    Return configuration for a Cell Painting dataset.
    - 3 compartments CSVs: Cells, Cytoplasm, Nuclei
    - 1 image CSV
    - No object.csv and therefore no munging
    """
    return {
        "config": "config.ini",  # default
        "config_ref": "config_ref.ini",  # reference option for schema is set to "path/to/reference/folder"
        "data_dir": "tests/data_b",
        "image_csv": "Image.csv",
        "ingest": [
            {"ncols": 586, "nrows": 40, "table": "Cells"},
            {"ncols": 572, "nrows": 40, "table": "Cytoplasm"},
            {"ncols": 6, "nrows": 4, "table": "image"},
            {"ncols": 595, "nrows": 40, "table": "Nuclei"},
        ],
        "munge": False,
        "skipped_dirs": ["E17-4", "J21-2", "N23-5"],
        "dropped_cols_optional": ["E17-4", "J21-2", "N23-5"],
    }


"""
Note: Some of the tables in "tests/data_b" were manipulated on purpose to check the handling of erronuous files.
In that case the utils.validate_csv_set() should raise an IO-error, and the entire directory
in which the broken file resides will not be ingested. To test the values of the ingested files, 
these directories must be known to the testing functions; the user is informed about invalid
filed with the following print:

Some files were invalid: Cells.csv,Nuclei.csv. Skipping tests/data_b/E17-4.
Some files were invalid: Cells.csv. Skipping tests/data_b/J21-2.
Some files were invalid: Image.csv. Skipping tests/data_b/N23-5.
"""


@pytest.fixture
def htqc():
    """
    Return configuration for a 3-channel image-based profiling dataset.
    - 1 object CSV that comprises Cells, Cytoplasm, Nuclei
    - 1 image CSV
    - munging required
    """
    return {
        "config": "config.ini",
        "data_dir": "tests/data_a",
        "munged_dir": "tests/data_a_munged",
        "image_csv": "image.csv",
        "ingest": [
            {"ncols": 294, "nrows": 40, "table": "Cells"},
            {"ncols": 279, "nrows": 40, "table": "Cytoplasm"},
            {"ncols": 229, "nrows": 8, "table": "image"},
            {"ncols": 287, "nrows": 40, "table": "Nuclei"},
        ],
        "munge": True,
        "skipped_dirs": [],
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
        "ingest": [{"nrows": 8, "ncols": 229, "table": "Image"}],
        "munge": True,
        "skipped_dirs": [],
    }


@pytest.fixture
def dataset(request):
    # Note that calling fixtures directly is deprecated:
    # https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
    # Instead, use the request fixture to dynamically run the named fixture function:
    # https://docs.pytest.org/en/latest/reference.html#_pytest.fixtures.FixtureRequest.getfixturevalue
    dataset_param = request.param

    if dataset_param == "htqc":
        return request.getfixturevalue("htqc")

    if dataset_param == "cellpainting":
        return request.getfixturevalue("cellpainting")

    if dataset_param == "qc":
        return request.getfixturevalue("qc")

    raise ValueError("No such dataset: {}".format(request.param))
