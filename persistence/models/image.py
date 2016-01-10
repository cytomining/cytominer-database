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

    matches = sqlalchemy.orm.relationship('Match', backref='images')
