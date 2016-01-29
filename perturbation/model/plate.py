"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Plate(perturbation.base.Base):
    """

    """

    __tablename__ = 'plates'

    images = sqlalchemy.orm.relationship('Image', backref='plates')

    well_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('wells.id'))

    well = sqlalchemy.orm.relationship('Well')

    barcode = sqlalchemy.Column(sqlalchemy.Integer)
