import click
import configparser
import os
import pkg_resources

import cytominer_database.ingest_variable_engine
import cytominer_database.munge

"""
Runs new code (ingest_variable_engine.py instead of ingest.py).
Two backend engines are available: Sqlite and Parquet. 
In effect, these options are read from the config file.
In terms of the command (and testing the command), 
the config file name needs to be specified 
(each backend choice has its own config file).
"""


@click.command(
    "ingest_new",
    help="""\
Import CSV files into a database.

SOURCE is a directory containing subdirectories that contain CSV files.

TARGET is a connection string for the database.
""",
)
@click.argument("source", type=click.Path(exists=True))
@click.argument("target", type=click.Path(writable=True))
@click.option(
    "-c",
    "--config-file",
    default=pkg_resources.resource_filename(
        "cytominer_database", os.path.join("config", "config_cellpainting.ini")
    ),
    help="Configuration file.",
    type=click.Path(exists=True),
)
@click.option(
    "--munge/--no-munge",
    default=True,
    help="""\
True if the CSV files for individual compartments \
have been merged into a single CSV file; \
the CSV will be split into one CSV per compartment \
(Default: true).
""",
)
@click.option(
    "--skip-image-prefix/--no-skip-image-prefix",
    default=True,
    help="""\
True if the prefix of image table name should be \
excluded from the names of columns from per image \
table e.g. use  `Metadata_Plate` instead of \
`Image_Metadata_Plate` (Default: true).
""",
)

@click.option(
    "--variable-engine/--no-variable-engine",
    default=False,
    help="""\
True if multiple backend engines (SQLite or Parquet)\
can be selected. The config file then determines
which backend engine is used (path of which is passed as a flag).\
Default: False (--no-variable-engine) 
""",
)

@click.option(
    "--parquet/--no-parquet",
    default=False,
    help="""\
True if Parquet backend is selected (files are ingested to be Parquet files).
Default: False (--no-parquet) 
""",
)

@click.option(
    "--sqlite/--no-sqlite",
    default=False,
    help="""\
True if SQLite backend is selected (files are ingested to be Parquet files).
Default: False (--no-sqlite) 
""",
)


def command(source, target, config_file, munge, skip_image_prefix, parquet, sqlite):
    if munge:
        cytominer_database.munge.munge(config_path=config_file, source=source)

    if parquet and sqlite:
        raise ValueError(
                " Two command flags '--parquet' and '--sqlite' cannot be added simultaneously."
            )

    cytominer_database.ingest_variable_engine.seed(
        source, target, config_file, skip_image_prefix, parquet, sqlite
        )

