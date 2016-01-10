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

    abscissa = sqlalchemy.Column(sqlalchemy.Integer)

    ordinate = sqlalchemy.Column(sqlalchemy.Integer)
