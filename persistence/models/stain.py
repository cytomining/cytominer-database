"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Stain(persistence.base.Base):
    """

    """

    __tablename__ = 'stains'

    correlations = sqlalchemy.orm.relationship('Correlation', backref='stains')

    description = sqlalchemy.Column(sqlalchemy.Text)

    intensity = sqlalchemy.orm.relationship('Intensity', backref='stains', uselist=False)

    location = sqlalchemy.orm.relationship('Location', backref='stains', uselist=False)

    radial_distribution = sqlalchemy.orm.relationship('RadialDistribution', backref='stains', uselist=False)

    texture = sqlalchemy.orm.relationship('Texture', backref='stains', uselist=False)
