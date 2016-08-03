
import pytest

def pytest_addoption(parser):
    parser.addoption("--dataset", action="store", default="htqc", help="dataset to test")


@pytest.fixture
def selected_dataset(request):
    return request.config.getoption("--dataset")