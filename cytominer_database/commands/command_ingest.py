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

TARGET is a connection string for the database.
"""
)
@click.argument(
    "source",
    type=click.Path(exists=True)
)
@click.argument(
    "target",
    type=click.Path(writable=True)
)
@click.option(
    "-c",
    "--config-file",
    default=pkg_resources.resource_filename(
        "cytominer_database",
        os.path.join("config", "config_cellpainting.ini")
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
@click.option(
    "--skip-image-prefix/--no-skip-image-prefix",
    default=False,
    help="""\
True if the prefix of image table name should be
excluded from the names of columns from per image
table e.g. use  `Metadata_Plate` instead of
`Image_Metadata_Plate`.
"""
)
def command(source, target, config_file, munge, skip_image_prefix):
    config = configparser.ConfigParser()

    with open(config_file, "r") as config_fd:
        config.read_file(config_fd)

    if munge:
        cytominer_database.munge.munge(config=config, source=source)

    cytominer_database.ingest.seed(source, target, config,
        skip_image_prefix)
