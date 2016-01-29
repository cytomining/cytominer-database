"""

"""

import statistics


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

        return statistics.stdev(self.sample)
