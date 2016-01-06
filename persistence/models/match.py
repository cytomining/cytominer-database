"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Match(persistence.base.Base):
    """

    """

    __tablename__ = 'matches'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    object_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('objects.id'))

    neighborhood = sqlalchemy.orm.relationship('Neighborhood', backref='matches', uselist=False)

    shape = sqlalchemy.orm.relationship('Shape', backref='matches', uselist=False)

    stains = sqlalchemy.orm.relationship('Stain', backref='matches')
