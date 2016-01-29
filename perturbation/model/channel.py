"""

"""

import perturbation.base
import sqlalchemy


class Channel(perturbation.base.Base):
    """

    """

    __tablename__ = 'channels'

    description = sqlalchemy.Column(sqlalchemy.String)
