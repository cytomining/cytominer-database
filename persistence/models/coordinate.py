"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Coordinate(persistence.base.Base):
    """

    """

    __tablename__ = 'coordinates'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    x = sqlalchemy.Column(sqlalchemy.Integer)

    y = sqlalchemy.Column(sqlalchemy.Integer)

    z = sqlalchemy.Column(sqlalchemy.Integer)
