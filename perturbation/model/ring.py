"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Ring(perturbation.base.Base):
    """

    """

    __tablename__ = 'rings'

    radial_distributions = sqlalchemy.orm.relationship('RadialDistribution', backref='rings')
