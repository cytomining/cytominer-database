"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Location(perturbation.base.Base):
    """

    """

    __tablename__ = 'locations'

    center_mass_intensity_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinates.id'))

    center_mass_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[center_mass_intensity_id])

    channel_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('channels.id'))

    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))

    match = sqlalchemy.orm.relationship('Match')

    max_intensity_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinates.id'))

    max_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[max_intensity_id])
