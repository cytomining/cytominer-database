"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Moment(persistence.base.Base):
    """

    """

    __tablename__ = 'moments'

    shape_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('shapes.id'))
    shape = sqlalchemy.orm.relationship('Shape')

    a = sqlalchemy.Column(sqlalchemy.Integer)

    b = sqlalchemy.Column(sqlalchemy.Integer)

    score = sqlalchemy.Column(sqlalchemy.Float)
