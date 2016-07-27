"""

"""
import click
import glob
import hashlib
import os
import pandas
import collections
import logging
import logging.config
import perturbation.base
import perturbation.models
import perturbation.UUID
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.types
import uuid


@click.command()
@click.argument('connection', type=click.Path(exists=False, file_okay=False, readable=True))
@click.help_option(help='')
def main(connection):
    import json

    with open("logging_config.json") as f:
        logging.config.dictConfig(json.load(f))

    logger = logging.getLogger(__name__)

    logging.debug("Inspect")

    Base = perturbation.base.Base

    Session = sqlalchemy.orm.sessionmaker()

    engine = sqlalchemy.create_engine(connection)

    scoped_session = sqlalchemy.orm.scoped_session(Session)

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    import IPython

    IPython.embed()

