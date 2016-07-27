import os
import glob
import logging
import perturbation.base
import perturbation.models
import perturbation.seed_images
import perturbation.seed_objects
import perturbation.UUID
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.types


logger = logging.getLogger(__name__)


def setup(connection):
    """
    """

    Session = sqlalchemy.orm.sessionmaker()

    scoped_session = sqlalchemy.orm.scoped_session(Session)

    engine = sqlalchemy.create_engine(connection)

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    return scoped_session, engine


def seed(input, output, sqlfile=None):
    """Call functions to create backend

    :param input: if stage is `images`, then top-level directory containing sub-directories, each of which have an
    image.csv and object.csv; if stage is 'objects', then a subdirectory contain a pair of image.csv and object.csv
    :param output: name of SQLlite/PostGreSQL database
    :param sqlfile: SQL file to be executed on the backend database after it is created
    :return:
    """

    scoped_session, engine = setup(output)

    Base = perturbation.base.Base

    Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)

    logger.debug('Parsing SQL file')

    if sqlfile:
        create_views(sqlfile, engine)

    logger.debug('Parsing: pass 1')

    perturbation.seed_images.seed(input, scoped_session)

    logger.debug('Parsing: pass 2')

    for directory in glob.glob(os.path.join(input, '*/')):
        perturbation.seed_objects.seed(directory, scoped_session)


def create_views(sqlfile, engine):
    with open(sqlfile) as f:
        import sqlparse

        for s in sqlparse.split(f.read()):
            engine.execute(s)






