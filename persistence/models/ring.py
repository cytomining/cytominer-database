"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Ring(persistence.base.Base):
    """

    """

    __tablename__ = 'rings'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
