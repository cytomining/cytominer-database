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

    @classmethod
    def find_or_create_by(cls, session, create_method='', create_method_kwargs=None, **kwargs):
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
