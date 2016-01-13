"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Image(perturbation.base.Base):
    """

    """

    __tablename__ = 'images'

    plate_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('plates.id'))
    plate = sqlalchemy.orm.relationship('Plate')

    matches = sqlalchemy.orm.relationship('Match', backref='images')
