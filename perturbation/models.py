"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.orm.exc


class Channel(perturbation.base.Base):
    """

    """

    __tablename__ = 'channels'

    description = sqlalchemy.Column(sqlalchemy.String, index=True)


class Coordinate(perturbation.base.Base):
    """

    """

    __tablename__ = 'coordinates'

    abscissa = sqlalchemy.Column(sqlalchemy.Integer)
    ordinate = sqlalchemy.Column(sqlalchemy.Integer)


class Correlation(perturbation.base.Base):
    """

    """

    __tablename__ = 'correlations'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    dependent_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    dependent = sqlalchemy.orm.relationship('Channel', foreign_keys=[dependent_id])

    independent_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    independent = sqlalchemy.orm.relationship('Channel', foreign_keys=[independent_id])

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    coefficient = sqlalchemy.Column(sqlalchemy.Float)


class Edge(perturbation.base.Base):
    """

    """

    __tablename__ = 'edges'

    channel_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    integrated = sqlalchemy.Column(sqlalchemy.Float)

    maximum = sqlalchemy.Column(sqlalchemy.Float)

    mean = sqlalchemy.Column(sqlalchemy.Float)

    minimum = sqlalchemy.Column(sqlalchemy.Float)

    standard_deviation = sqlalchemy.Column(sqlalchemy.Float)


class Image(perturbation.base.Base):
    """

    """

    __tablename__ = 'images'

    objects = sqlalchemy.orm.relationship('Object')

    well_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('wells.id'), index=True)
    well = sqlalchemy.orm.relationship('Well', back_populates='images', uselist=False)

    description = sqlalchemy.Column(sqlalchemy.String, index=True)


class Intensity(perturbation.base.Base):
    """

    """

    __tablename__ = 'intensities'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    channel_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    first_quartile = sqlalchemy.Column(sqlalchemy.Float)

    integrated = sqlalchemy.Column(sqlalchemy.Float)

    mass_displacement = sqlalchemy.Column(sqlalchemy.Float)

    maximum = sqlalchemy.Column(sqlalchemy.Float)

    mean = sqlalchemy.Column(sqlalchemy.Float)

    median = sqlalchemy.Column(sqlalchemy.Float)

    median_absolute_deviation = sqlalchemy.Column(sqlalchemy.Float)

    minimum = sqlalchemy.Column(sqlalchemy.Float)

    standard_deviation = sqlalchemy.Column(sqlalchemy.Float)

    third_quartile = sqlalchemy.Column(sqlalchemy.Float)


class Location(perturbation.base.Base):
    """

    """

    __tablename__ = 'locations'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    center_mass_intensity_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('coordinates.id'), index=True)
    center_mass_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[center_mass_intensity_id])

    channel_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    max_intensity_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('coordinates.id'), index=True)
    max_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[max_intensity_id])


class Match(perturbation.base.Base):
    """

    """

    __tablename__ = 'matches'

    center_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('coordinates.id'), index=True)
    center = sqlalchemy.orm.relationship('Coordinate')

    correlations = sqlalchemy.orm.relationship('Correlation', backref='matches')

    edges = sqlalchemy.orm.relationship('Edge', backref='matches')

    intensities = sqlalchemy.orm.relationship('Intensity', backref='matches')

    locations = sqlalchemy.orm.relationship('Location', backref='matches')

    neighborhood_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('neighborhoods.id'), index=True)
    neighborhood = sqlalchemy.orm.relationship('Neighborhood')

    object_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('objects.id'), index=True)
    object = sqlalchemy.orm.relationship('Object')

    pattern_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('patterns.id'), index=True)
    pattern = sqlalchemy.orm.relationship('Pattern')

    radial_distributions = sqlalchemy.orm.relationship('RadialDistribution', backref='matches')

    shape_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('shapes.id'), index=True)
    shape = sqlalchemy.orm.relationship('Shape')

    textures = sqlalchemy.orm.relationship('Texture', backref='matches')


class Metadata(perturbation.base.Base):
    """

    """

    __tablename__ = 'metadata'

    image_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('images.id'), index=True)
    image = sqlalchemy.orm.relationship('Image')

    is_cell_clump = sqlalchemy.Column(sqlalchemy.Integer)

    is_debris = sqlalchemy.Column(sqlalchemy.Integer)

    is_low_intensity = sqlalchemy.Column(sqlalchemy.Integer)


class Moment(perturbation.base.Base):
    """

    """

    __tablename__ = 'moments'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    shape_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('shapes.id'), index=True)
    shape = sqlalchemy.orm.relationship('Shape')

    a = sqlalchemy.Column(sqlalchemy.Integer)

    b = sqlalchemy.Column(sqlalchemy.Integer)

    score = sqlalchemy.Column(sqlalchemy.Float)


class Neighborhood(perturbation.base.Base):
    """

    """

    __tablename__ = 'neighborhoods'

    closest_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('objects.id'), index=True)
    closest = sqlalchemy.orm.relationship('Object', foreign_keys=[closest_id])

    object_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('objects.id'), index=True)
    object = sqlalchemy.orm.relationship('Object', foreign_keys=[object_id])

    second_closest_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('objects.id'), index=True)
    second_closest = sqlalchemy.orm.relationship('Object', foreign_keys=[second_closest_id])

    match = sqlalchemy.orm.relationship('Match', uselist=False)

    angle_between_neighbors_5 = sqlalchemy.Column(sqlalchemy.Float)

    angle_between_neighbors_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_distance_5 = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_distance_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_object_number_adjacent = sqlalchemy.Column(sqlalchemy.Integer)

    number_of_neighbors_5 = sqlalchemy.Column(sqlalchemy.Integer)

    number_of_neighbors_adjacent = sqlalchemy.Column(sqlalchemy.Integer)

    percent_touching_5 = sqlalchemy.Column(sqlalchemy.Float)

    percent_touching_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_distance_5 = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_distance_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_object_number_adjacent = sqlalchemy.Column(sqlalchemy.Integer)


class Object(perturbation.base.Base):
    """

    """

    __tablename__ = 'objects'

    image_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('images.id'), index=True)
    image = sqlalchemy.orm.relationship('Image')

    matches = sqlalchemy.orm.relationship('Match')

    description = sqlalchemy.Column(sqlalchemy.Integer, index=True)


class Pattern(perturbation.base.Base):
    """

    """

    __tablename__ = 'patterns'

    matches = sqlalchemy.orm.relationship('Match', backref='patterns')

    description = sqlalchemy.Column(sqlalchemy.String, index=True)


class Plate(perturbation.base.Base):
    """

    """

    __tablename__ = 'plates'

    wells = sqlalchemy.orm.relationship('Well')

    description = sqlalchemy.Column(sqlalchemy.String, index=True)


class RadialDistribution(perturbation.base.Base):
    """

    """

    __tablename__ = 'radial_distributions'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    bins = sqlalchemy.Column(sqlalchemy.Integer)

    channel_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    frac_at_d = sqlalchemy.Column(sqlalchemy.Float)

    mean_frac = sqlalchemy.Column(sqlalchemy.Float)

    radial_cv = sqlalchemy.Column(sqlalchemy.Float)


class Shape(perturbation.base.Base):
    """

    """

    __tablename__ = 'shapes'

    center_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('coordinates.id'), index=True)
    center = sqlalchemy.orm.relationship('Coordinate', uselist=False)

    match = sqlalchemy.orm.relationship('Match', uselist=False)

    moments = sqlalchemy.orm.relationship('Moment')

    area = sqlalchemy.Column(sqlalchemy.Float)

    compactness = sqlalchemy.Column(sqlalchemy.Float)

    eccentricity = sqlalchemy.Column(sqlalchemy.Float)

    euler_number = sqlalchemy.Column(sqlalchemy.Float)

    extent = sqlalchemy.Column(sqlalchemy.Float)

    form_factor = sqlalchemy.Column(sqlalchemy.Float)

    major_axis_length = sqlalchemy.Column(sqlalchemy.Float)

    max_feret_diameter = sqlalchemy.Column(sqlalchemy.Float)

    maximum_radius = sqlalchemy.Column(sqlalchemy.Float)

    mean_radius = sqlalchemy.Column(sqlalchemy.Float)

    median_radius = sqlalchemy.Column(sqlalchemy.Float)

    min_feret_diameter = sqlalchemy.Column(sqlalchemy.Float)

    minor_axis_length = sqlalchemy.Column(sqlalchemy.Float)

    orientation = sqlalchemy.Column(sqlalchemy.Float)

    perimeter = sqlalchemy.Column(sqlalchemy.Float)

    solidity = sqlalchemy.Column(sqlalchemy.Float)


class Texture(perturbation.base.Base):
    """

    """

    __tablename__ = 'textures'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    channel_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('channels.id'), index=True)
    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('matches.id'), index=True)
    match = sqlalchemy.orm.relationship('Match')

    angular_second_moment = sqlalchemy.Column(sqlalchemy.Float)

    contrast = sqlalchemy.Column(sqlalchemy.Float)

    correlation = sqlalchemy.Column(sqlalchemy.Float)

    difference_entropy = sqlalchemy.Column(sqlalchemy.Float)

    difference_variance = sqlalchemy.Column(sqlalchemy.Float)

    scale = sqlalchemy.Column(sqlalchemy.Integer)

    entropy = sqlalchemy.Column(sqlalchemy.Float)

    gabor = sqlalchemy.Column(sqlalchemy.Float)

    info_meas_1 = sqlalchemy.Column(sqlalchemy.Float)

    info_meas_2 = sqlalchemy.Column(sqlalchemy.Float)

    inverse_difference_moment = sqlalchemy.Column(sqlalchemy.Float)

    sum_average = sqlalchemy.Column(sqlalchemy.Float)

    sum_entropy = sqlalchemy.Column(sqlalchemy.Float)

    sum_variance = sqlalchemy.Column(sqlalchemy.Float)

    variance = sqlalchemy.Column(sqlalchemy.Float)


class Well(perturbation.base.Base):
    """

    """

    __tablename__ = 'wells'

    images = sqlalchemy.orm.relationship('Image')

    plate_id = sqlalchemy.Column(perturbation.UUID.UUID, sqlalchemy.ForeignKey('plates.id'), index=True)
    plate = sqlalchemy.orm.relationship('Plate')

    description = sqlalchemy.Column(sqlalchemy.String, index=True)
