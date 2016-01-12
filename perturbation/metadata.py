"""

"""

import numpy
import pandas


class Metadata(object):
    """

    """

    def __init__(self, filename):
        self.filename = filename

        self.records = self.__records__()

    def __image__(self, identifier):
        return self.records[self.records[('Image', 'ImageNumber')] == identifier]

    def __matches__(self, identifier):
        return self.records[self.records[('Object', 'ObjectNumber')] == identifier]

    def __patterns__(self):
        patterns = []

        for pattern in self.records.columns:
            pattern = pattern[0]

            if pattern != 'Image':
                patterns.append(pattern)

        patterns = numpy.unique(patterns)

        return patterns

    def __records__(self):
        records = pandas.read_csv(self.filename, header=[0, 1], tupleize_cols=True)

        records.rename(columns={records.columns[1]: ('Object', 'ObjectNumber')}, inplace=True)

        return records

    def __find__(self, image, match):
        images = self.records[self.records[('Image', 'ImageNumber')] == image]

        return images[images[('Object', 'ObjectNumber')] == match][('Cells', 'AreaShape_Area')]

    def __shape__(self, pattern, image, match):
        pass
