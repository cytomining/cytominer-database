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

    texture_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('textures.id'))
    texture = sqlalchemy.orm.relationship('Texture')

    degree = sqlalchemy.Column(sqlalchemy.Integer)
