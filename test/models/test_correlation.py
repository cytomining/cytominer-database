"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Correlation(perturbation.base.Base):
    """

    """

    __tablename__ = 'correlations'

    dependent_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))
    dependent = sqlalchemy.orm.relationship('Stain', foreign_keys=[dependent_id])

    independent_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))
    independent = sqlalchemy.orm.relationship('Stain', foreign_keys=[independent_id])

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))
    match = sqlalchemy.orm.relationship('Match')

    coefficient = sqlalchemy.Column(sqlalchemy.Float)
