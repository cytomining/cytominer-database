"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Neighborhood(perturbation.base.Base):
    """

    """

    __tablename__ = 'neighborhoods'

    match = sqlalchemy.orm.relationship('Match', backref='neighborhoods', uselist=False)

    angle_between_neighbors_5 = sqlalchemy.Column(sqlalchemy.Float)

    angle_between_neighbors_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_distance_5 = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_distance_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_object_number_5 = sqlalchemy.Column(sqlalchemy.Float)

    first_closest_object_number_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    number_of_neighbors_5 = sqlalchemy.Column(sqlalchemy.Float)

    number_of_neighbors_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    percent_touching_5 = sqlalchemy.Column(sqlalchemy.Float)

    percent_touching_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_distance_5 = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_distance_adjacent = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_object_number_5 = sqlalchemy.Column(sqlalchemy.Float)

    second_closest_object_number_adjacent = sqlalchemy.Column(sqlalchemy.Float)
