import pandas
import persistence.base
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.exc

engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)

Session = sqlalchemy.orm.sessionmaker(bind=engine)

persistence.base.Base.metadata.create_all(engine)

connection = Session()


def find_or_create_by(session, model, create_method='', create_method_kwargs=None, **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one()
    except sqlalchemy.orm.exc.NoResultFound:
        kwargs.update(create_method_kwargs or {})

        created = getattr(model, create_method, model)(**kwargs)

        try:
            session.add(created)

            session.flush()

            return created
        except sqlalchemy.exc.IntegrityError:
            session.rollback()

            return session.query(model).filter_by(**kwargs).one()


def identify(filename):
    with open(filename) as fixture:
        columns = pandas.read_csv(fixture, header=None, nrows=2)

        names = []

        for prefix, suffix in zip(columns[:1].squeeze(), columns[1:].squeeze()):
            names.append('{}_{}'.format(prefix, suffix))

        return names


def observe(filename):
    with open(filename) as fixture:
        return pandas.read_csv(fixture, header=None, names=identify(filename), skiprows=2)

if __name__ == '__main__':
    print('…')
