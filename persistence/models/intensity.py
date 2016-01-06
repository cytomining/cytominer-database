"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Intensity(persistence.base.Base):
    """

    """

    __tablename__ = 'intensities'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    stain_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('stains.id'))

    stain = sqlalchemy.orm.relationship('Stain')

    integrated_intensity = sqlalchemy.Column(sqlalchemy.Float)

    integrated_intensity_edge = sqlalchemy.Column(sqlalchemy.Float)

    lower_quartile_intensity = sqlalchemy.Column(sqlalchemy.Float)

    mad_intensity = sqlalchemy.Column(sqlalchemy.Float)

    mass_displacement = sqlalchemy.Column(sqlalchemy.Float)

    max_intensity = sqlalchemy.Column(sqlalchemy.Float)

    max_intensity_edge = sqlalchemy.Column(sqlalchemy.Float)

    mean_intensity = sqlalchemy.Column(sqlalchemy.Float)

    mean_intensity_edge = sqlalchemy.Column(sqlalchemy.Float)

    median_intensity = sqlalchemy.Column(sqlalchemy.Float)

    min_intensity = sqlalchemy.Column(sqlalchemy.Float)

    min_intensity_edge = sqlalchemy.Column(sqlalchemy.Float)

    std_intensity = sqlalchemy.Column(sqlalchemy.Float)

    std_intensity_edge = sqlalchemy.Column(sqlalchemy.Float)

    upper_quartile_intensity = sqlalchemy.Column(sqlalchemy.Float)
