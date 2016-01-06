"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Correlation(persistence.base.Base):
    """

    """

    __tablename__ = 'correlations'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))

    stain = sqlalchemy.orm.relationship('Stain')

    y = sqlalchemy.Column(sqlalchemy.Text)

    coefficient = sqlalchemy.Column(sqlalchemy.Float)
