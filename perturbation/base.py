"""

"""

import sqlalchemy.ext.declarative
import perturbation.UUID
import uuid


@sqlalchemy.ext.declarative.as_declarative()
class Base(object):
    """

    """

    id = sqlalchemy.Column(perturbation.UUID.UUID, default=uuid.uuid4, primary_key=True)

    @classmethod
    def find_or_create_by(cls, session, create_method='', create_method_kwargs=None, **kwargs):
        """

        :param session:
        :param create_method:
        :param create_method_kwargs:

        :return:

        """

        try:
            return session.query(cls).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            kwargs.update(create_method_kwargs or {})

            created = getattr(cls, create_method, cls)(**kwargs)

            try:
                session.add(created)

                session.flush()

                return created
            except sqlalchemy.exc.IntegrityError:
                session.rollback()

                return session.query(cls).filter_by(**kwargs).one()
