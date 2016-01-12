"""

"""

import statistics


class StandardDeviation(object):
    """

    """

    def __init__(self):
        self.elements = []

    def step(self, element):
        self.elements.append(element)

    def finalize(self):
        return statistics.stdev(self.elements)
