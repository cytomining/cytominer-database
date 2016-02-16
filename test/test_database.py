"""

"""

import os
import perturbation.base
import perturbation.database
import perturbation.models
import pytest
import sqlalchemy
import sqlalchemy.orm
import subprocess


@pytest.fixture
def session(output='/tmp/test.sqlite'):
    try:
        os.remove(output)
    except OSError:
        pass

    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    perturbation.base.Base.metadata.create_all(engine)

    return session()


def test_seed(session):
    subprocess.run(['source', 'munge.sh', 'test/data/'])

    perturbation.database.seed('test/data/', '/tmp/test.sqlite')

    assert len(session.query(perturbation.models.Match).all()) == 60
