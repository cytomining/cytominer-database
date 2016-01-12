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

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))
    match = sqlalchemy.orm.relationship('Match')

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))
    stain = sqlalchemy.orm.relationship('Stain')

    center_mass_intensity_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinates.id'))
    center_mass_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[center_mass_intensity_id])

    max_intensity_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('coordinates.id'))
    max_intensity = sqlalchemy.orm.relationship('Coordinate', foreign_keys=[max_intensity_id])
