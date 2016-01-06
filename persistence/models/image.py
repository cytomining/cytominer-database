"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Image(persistence.base.Base):
    """

    """

    __tablename__ = 'images'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    objects = sqlalchemy.orm.relationship('Object', backref='images')
