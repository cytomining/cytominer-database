"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Match(perturbation.base.Base):
    """

    """

    __tablename__ = 'matches'

    image_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('images.id'))
    image = sqlalchemy.orm.relationship('Image')

    neighborhood_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('neighborhoods.id'))
    neighborhood = sqlalchemy.orm.relationship('Neighborhood')

    pattern_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('patterns.id'))
    pattern = sqlalchemy.orm.relationship('Pattern')

    shape_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('shapes.id'))
    shape = sqlalchemy.orm.relationship('Shape')

    correlations = sqlalchemy.orm.relationship('Correlation', backref='matches')

    intensities = sqlalchemy.orm.relationship('Intensity', backref='matches')

    locations = sqlalchemy.orm.relationship('Location', backref='matches')

    radial_distributions = sqlalchemy.orm.relationship('RadialDistribution', backref='matches')

    textures = sqlalchemy.orm.relationship('Texture', backref='matches')
