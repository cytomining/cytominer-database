"""

"""


class StandardScore(object):
    """

    """

    def __init__(self):
        self.scores = []

    def step(self, observation, μ, σ):
        self.scores.append(observation - μ / σ)

    def finalize(self):
        return self.scores
