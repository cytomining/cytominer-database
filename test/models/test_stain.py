"""

"""

import perturbation.base
import sqlalchemy


class Stain(perturbation.base.Base):
    """

    """

    __tablename__ = 'stains'

    description = sqlalchemy.Column(sqlalchemy.String)
