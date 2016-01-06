"""

"""

import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Object(persistence.base.Base):
    """

    """

    __tablename__ = 'objects'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    image_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('images.id'))

    image = sqlalchemy.orm.relationship('Image')

    matches = sqlalchemy.orm.relationship('Match', backref='objects')

    description = sqlalchemy.Column(sqlalchemy.Text)
