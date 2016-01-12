"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Shape(perturbation.base.Base):
    """

    """

    __tablename__ = 'shapes'

    center_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinates.id'))
    center = sqlalchemy.orm.relationship('Coordinate', backref='shapes', uselist=False)

    match = sqlalchemy.orm.relationship('Match', backref='shapes', uselist=False)

    moments = sqlalchemy.orm.relationship('Moment', backref='shapes')

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
