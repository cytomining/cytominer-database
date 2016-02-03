"""

"""

import numpy


def standardize(observation, μ, σ):
    """

    :param observation:
    :param μ:
    :param σ:

    :return:

    """

    return observation - μ / σ


class StandardDeviation(object):
    """

    """

    def __init__(self):
        """

        :return:

        """

        self.sample = []

    def step(self, observation):
        """

        :param observation:

        :return:

        """

        self.sample.append(observation)

    def finalize(self):
        """

        :return:

        """

        return numpy.std(self.sample)
