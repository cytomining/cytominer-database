"""

"""

import numpy


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


class StandardScore(object):
    """

    """

    def __init__(self):
        """

        :return:

        """

        self.scores = []

    def step(self, observation, μ, σ):
        """

        :param observation:
        :param μ:
        :param σ:

        :return:

        """

        self.scores.append(observation - μ / σ)

    def finalize(self):
        """

        :return:

        """

        return self.scores
