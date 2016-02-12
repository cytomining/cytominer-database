"""

"""

import perturbation.models
import pytest


@pytest.fixture
def image():
    return perturbation.models.Image()


@pytest.fixture
def well():
    return perturbation.models.Well()


class TestImage:
    """

    """

    def test_well(self, image, well):
        image.well = well

        assert image.well == well


class TestWell:
    """

    """

    def test_image(self, image, well):
        well.images.append(image)

        assert well.images == [image]
