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


def __main__():
    pass
