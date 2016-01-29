"""

"""

import perturbation.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Scale(perturbation.base.Base):
    """

    """

    __tablename__ = 'scales'

    parameter = sqlalchemy.Column(sqlalchemy.Integer)
