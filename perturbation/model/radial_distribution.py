"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class RadialDistribution(perturbation.base.Base):
    """

    """

    __tablename__ = 'radial_distributions'

    channel_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('channels.id'))

    channel = sqlalchemy.orm.relationship('Channel')

    match_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('matches.id'))

    match = sqlalchemy.orm.relationship('Match')

    ring_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('rings.id'))

    ring = sqlalchemy.orm.relationship('Ring')

    frac_at_d = sqlalchemy.Column(sqlalchemy.Float)

    mean_frac = sqlalchemy.Column(sqlalchemy.Float)

    radial_cv = sqlalchemy.Column(sqlalchemy.Float)
