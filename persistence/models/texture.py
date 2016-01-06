"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Texture(persistence.base.Base):
    """

    """

    __tablename__ = 'textures'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    angle = sqlalchemy.orm.relationship('Angle')

    angle_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('angles.id'))

    stain = sqlalchemy.orm.relationship('Stain')

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))

    angular_second_moment = sqlalchemy.Column(sqlalchemy.Float)

    contrast = sqlalchemy.Column(sqlalchemy.Float)

    correlation = sqlalchemy.Column(sqlalchemy.Float)

    difference_entropy = sqlalchemy.Column(sqlalchemy.Float)

    difference_variance = sqlalchemy.Column(sqlalchemy.Float)

    entropy = sqlalchemy.Column(sqlalchemy.Float)

    gabor = sqlalchemy.Column(sqlalchemy.Float)

    info_meas_1 = sqlalchemy.Column(sqlalchemy.Float)

    info_meas_2 = sqlalchemy.Column(sqlalchemy.Float)

    inverse_difference_moment = sqlalchemy.Column(sqlalchemy.Float)

    sum_average = sqlalchemy.Column(sqlalchemy.Float)

    sum_entropy = sqlalchemy.Column(sqlalchemy.Float)

    sum_variance = sqlalchemy.Column(sqlalchemy.Float)

    variance = sqlalchemy.Column(sqlalchemy.Float)
