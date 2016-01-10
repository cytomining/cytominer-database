"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Pattern(persistence.base.Base):
    """

    """

    __tablename__ = 'patterns'

    matches = sqlalchemy.orm.relationship('Match', backref='patterns')

    description = sqlalchemy.Column(sqlalchemy.String)
