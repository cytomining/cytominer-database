import pytest


def pytest_addoption(parser):
    parser.addoption("--dataset", action="store", help="dataset to test")


def pytest_generate_tests(metafunc):
    if "dataset" in metafunc.fixturenames:
        if metafunc.config.option.dataset is None:
            metafunc.parametrize("dataset", ["htqc", "cellpainting"], indirect=True)
        else:
            assert metafunc.config.option.dataset in ["htqc", "cellpainting"]

            metafunc.parametrize("dataset", [metafunc.config.option.dataset], indirect=True)


@pytest.fixture
def cellpainting():
    return {
        "data_dir": "tests/data_b",
        "image_csv": "Image.csv",
        "ingest":
            {
                "Image_nrows": 4,
                "Image_ncols": 6,
                "Cells_nrows": 40,
                "Cells_ncols": 586,
                "Cytoplasm_nrows": 40,
                "Cytoplasm_ncols": 572,
                "Nuclei_nrows": 40,
                "Nuclei_ncols": 595
            },
        "munge": False
    }


@pytest.fixture
def htqc():
    return {
        "data_dir": "tests/data_a",
        "munged_dir": "tests/data_a_munged",
        "image_csv": "image.csv",
        "ingest":
            {
                "image_nrows": 8,
                "image_ncols": 229,
                "Cells_nrows": 40,
                "Cells_ncols": 294,
                "Cytoplasm_nrows": 40,
                "Cytoplasm_ncols": 279,
                "Nuclei_nrows": 40,
                "Nuclei_ncols": 287
            },
        "munge": True
    }


@pytest.fixture
def dataset(request):
    if request.param == "htqc":
        return htqc()

    if request.param == "cellpainting":
        return cellpainting()

    raise ValueError("No such dataset: {}".format(request.param))
