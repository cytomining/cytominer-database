"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Location(persistence.base.Base):
    """

    """

    __tablename__ = 'locations'

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))
    match = sqlalchemy.orm.relationship('Match')

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))
    stain = sqlalchemy.orm.relationship('Stain')

    center_mass_intensity_x = sqlalchemy.Column(sqlalchemy.Float)

    center_mass_intensity_y = sqlalchemy.Column(sqlalchemy.Float)

    max_intensity_x = sqlalchemy.Column(sqlalchemy.Float)

    max_intensity_y = sqlalchemy.Column(sqlalchemy.Float)
