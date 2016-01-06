"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Shape(persistence.base.Base):
    """

    """

    __tablename__ = 'shapes'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))

    match = sqlalchemy.orm.relationship('Match')

    moments = sqlalchemy.orm.relationship('Moment', backref='shapes')

    area = sqlalchemy.Column(sqlalchemy.Float)

    coordinate_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinate.id'))

    center = sqlalchemy.orm.relationship('Coordinate', backref='shapes', uselist=False)

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
