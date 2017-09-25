import click
import configparser
import os
import pkg_resources

import cytominer_database.ingest
import cytominer_database.munge


@click.command(
    "ingest",
    help="""\
Import CSV files into a database.

SOURCE is a directory containing subdirectories that contain CSV files.
"""
)
@click.argument(
    "source",
    type=click.Path(exists=True)
)
@click.option(
    "-c",
    "--config-file",
    default=pkg_resources.resource_filename(
        __name__,
        os.path.join("config", "config_htqc.ini")
    ),
    help="Configuration file.",
    type=click.Path(exists=True)
)
@click.option(
    "--munge/--no-munge",
    default=True,
    help="""\
True if the CSV files for individual compartments
have been merged into a single CSV file. If True,
the CSV will be split into one CSV per compartment.
"""
)
@click.option(  # TODO: Is this an argument/required?
    "-o",
    "--target",
    help="Connection string for the database.",
    type=click.Path(writable=True)
)
def command(source, config_file, munge, target):
    config = configparser.ConfigParser()

    config.read(config_file)

    if munge:
        cytominer_database.munge.munge(config=config, source=source)

    cytominer_database.ingest.seed(source, target, config)
