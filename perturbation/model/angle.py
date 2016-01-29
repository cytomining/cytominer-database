"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Angle(perturbation.base.Base):
    """

    """

    __tablename__ = 'angles'

    textures = sqlalchemy.orm.relationship('Texture', backref='angles')

    degree = sqlalchemy.Column(sqlalchemy.Integer)
