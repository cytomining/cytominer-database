"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Angle(persistence.base.Base):
    """

    """

    __tablename__ = 'angles'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    degree = sqlalchemy.Column(sqlalchemy.Integer)
