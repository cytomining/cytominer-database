"""

"""

import numpy
import pandas
import perturbation.base
import perturbation.model.angle
import perturbation.model.coordinate
import perturbation.model.correlation
import perturbation.model.image
import perturbation.model.intensity
import perturbation.model.location
import perturbation.model.match
import perturbation.model.moment
import perturbation.model.neighborhood
import perturbation.model.pattern
import perturbation.model.radial_distribution
import perturbation.model.ring
import perturbation.model.scale
import perturbation.model.shape
import perturbation.model.stain
import perturbation.model.texture
import sqlalchemy
import sqlalchemy.orm


class Metadata(object):
    """

    """

    def __init__(self, filename):
        self.engine = sqlalchemy.create_engine('sqlite://')

        self.Session = sqlalchemy.orm.sessionmaker(bind=self.engine)

        self.session = self.Session()

        self.filename = filename

        self.patterns = []

        self.images = []

        self.records = self.__records__()

        perturbation.base.Base.metadata.create_all(self.engine)

    def __images__(self, identifier):
        return self.records[self.records[('Image', 'ImageNumber')] == identifier]

    def __matches__(self, identifier):
        return self.records[self.records[('Object', 'ObjectNumber')] == identifier]

    def __patterns__(self):
        patterns = []

        for pattern in self.records.columns:
            pattern = pattern[0]

            if pattern != 'Image':
                if pattern != 'Object':
                    patterns.append(pattern)

        return numpy.unique(patterns)

    def __records__(self):
        records = pandas.read_csv(self.filename, header=[0, 1], tupleize_cols=True)

        records.rename(columns={records.columns[1]: ('Object', 'ObjectNumber')}, inplace=True)

        return records

    def __find__(self, image, match):
        images = self.records[self.records[('Image', 'ImageNumber')] == image]

        return images[images[('Object', 'ObjectNumber')] == match][('Cells', 'AreaShape_Area')]

    def __image__(self, record):
        return perturbation.model.image.Image().find_or_create_by(
            session=self.session,

            id=record[('Image', 'ImageNumber')]
        )

    def __match__(self, identifier):
        return perturbation.model.match.Match()

    def __shape__(self, feature, match):
        record = self.__matches__(match)

        return perturbation.model.shape.Shape().find_or_create_by(
            session=self.session,

            center=perturbation.model.coordinate.Coordinate.find_or_create_by(
                session=self.session,

                abscissa=record[(feature, 'AreaShape_Center_X')],
                ordinate=record[(feature, 'AreaShape_Center_Y')]
            ),

            area=record[(feature, 'AreaShape_Area')],
            compactness=record[(feature, 'AreaShape_Compactness')],
            eccentricity=record[(feature, 'AreaShape_Eccentricity')],
            euler_number=record[(feature, 'AreaShape_EulerNumber')],
            extent=record[(feature, 'AreaShape_Extent')],
            form_factor=record[(feature, 'AreaShape_FormFactor')],
            major_axis_length=record[(feature, 'AreaShape_MajorAxisLength')],
            max_feret_diameter=record[(feature, 'AreaShape_MaxFeretDiameter')],
            maximum_radius=record[(feature, 'AreaShape_MaximumRadius')],
            mean_radius=record[(feature, 'AreaShape_MeanRadius')],
            median_radius=record[(feature, 'AreaShape_MedianRadius')],
            min_feret_diameter=record[(feature, 'AreaShape_MinFeretDiameter')],
            minor_axis_length=record[(feature, 'AreaShape_MinorAxisLength')],
            orientation=record[(feature, 'AreaShape_Orientation')],
            perimeter=record[(feature, 'AreaShape_Perimeter')],
            solidity=record[(feature, 'AreaShape_Solidity')]
        )
