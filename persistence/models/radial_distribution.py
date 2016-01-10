"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class RadialDistribution(persistence.base.Base):
    """

    """

    __tablename__ = 'radial_distributions'

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))
    match = sqlalchemy.orm.relationship('Match')

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))
    stain = sqlalchemy.orm.relationship('Stain')

    rings = sqlalchemy.orm.relationship('Ring', backref='radial_distributions')

    frac_at_d = sqlalchemy.Column(sqlalchemy.Float)

    mean_frac = sqlalchemy.Column(sqlalchemy.Float)

    radial_cv = sqlalchemy.Column(sqlalchemy.Float)
