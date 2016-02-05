import pytest

@pytest.fixture
def foo():
    return 1
    
def test_foo(foo):
    res = foo
    assert res == 1
