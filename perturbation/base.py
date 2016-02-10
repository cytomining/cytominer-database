"""

"""

import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc


@sqlalchemy.ext.declarative.as_declarative()
class Base(object):
    """

    """

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    def __init__(self, **kargs):
        self.__init__(kargs)

    @classmethod
    def all(cls, session):
        """

        :param session:

        :return:

        """

        return session.query(cls).all()

    @classmethod
    def exists(cls, session, **kwargs):
        """

        :param session:

        :return:

        """

        return session.query(cls).filter_by(**kwargs).exists()

    @classmethod
    def find(cls, session, identifier):
        """

        :param session:
        :param identifier:

        :return:

        """

        return session.query(cls).get(identifier)

    @classmethod
    def find_by(cls, session, **kwargs):
        """

        :param session:
        :param kwargs:

        :return:

        """

        return session.query(cls).filter_by(**kwargs).first()

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

    @classmethod
    def create_by(cls, session, create_method='', create_method_kwargs=None, **kwargs):
        """

        :param session:
        :param create_method:
        :param create_method_kwargs:

        :return:

        """

        kwargs.update(create_method_kwargs or {})

        created = getattr(cls, create_method, cls)(**kwargs)

        try:
            session.add(created)

            session.flush()

            return created
        except sqlalchemy.exc.IntegrityError:
            session.rollback()

            return session.query(cls).filter_by(**kwargs).one()

    @classmethod
    def first(cls, session):
        """

        :param session:

        :return:

        """

        return session.query(cls).first()

    @classmethod
    def take(cls, session, maximum=1):
        """

        :param session:
        :param maximum:

        :return:

        """

        pass
