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

    radial_distribution_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('radial_distributions.id'))
    radial_distribution = sqlalchemy.orm.relationship('RadialDistribution')
