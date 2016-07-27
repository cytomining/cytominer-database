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


def seed(input, output, stage, sqlfile=None):
    """Call functions to create backend

    :param input: top-level directory containing sub-directories, each of which have an image.csv and object.csv
    :param output: name of SQLlite/PostGreSQL database
    :param stage: images or objects
    :param sqlfile: SQL file to be executed on the backend database after it is created
    :return:
    """

    Session = sqlalchemy.orm.sessionmaker()

    scoped_session = sqlalchemy.orm.scoped_session(Session)

    engine = sqlalchemy.create_engine(output)

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    Base = perturbation.base.Base

    if stage == 'images':
        Base.metadata.drop_all(engine)

        Base.metadata.create_all(engine)

        logger.debug('Parsing SQL file')

        if sqlfile:
            create_views(sqlfile, engine)

    logger.debug('Parsing csvs')

    # Temporarily disable PostGreSQL triggers so that bulk inserts are possible
    if engine.name == "postgresql":
        scoped_session.execute("SET session_replication_role = replica;")

    if stage == 'images':
        perturbation.seed_images.seed(input, scoped_session)
    elif stage == 'objects':
        perturbation.seed_object.seed(input, scoped_session)
    else:
        raise ValueError('Unknown stage {}'.format(stage))

    # enable PostGreSQL triggers
    if engine.name == "postgresql":
        scoped_session.execute("SET session_replication_role = DEFAULT;")

    scoped_session.connection().close()
    
    scoped_session.remove()


def create_views(sqlfile, engine):
    with open(sqlfile) as f:
        import sqlparse

        for s in sqlparse.split(f.read()):
            engine.execute(s)






