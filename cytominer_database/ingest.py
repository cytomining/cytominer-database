"""
A mechanism to ingest CSV files into a database.

In morphological profiling experiments, a CellProfiler pipeline is often run in parallel across multiple images and
produces a set of CSV files. For example, imaging a 384-well plate, with 9 sites per well, produces 384 * 9 images;
a CellProfiler process may be run on each image, resulting in a 384*9 output directories (each directory typically
contains one CSV file per compartment (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv) and one CSV file for per-image
measurements (e.g. Image.csv).

``cytominer_database.ingest.seed`` can be used to read all these CSV files into a database backend. SQLite is the
recommended engine, but ingest will likely also work with PostgreSQL and MySQL.

``cytominer_database.ingest.seed`` assumes a directory structure like shown below:

| plate_a/
|   set_1/
|       file_1.csv
|       file_2.csv
|       ...
|       file_n.csv
|   set_2/
|       file_1.csv
|       file_2.csv
|       ...
|       file_n.csv
|   ...
|   set_m/
|       file_1.csv
|       file_2.csv
|       ...
|       file_n.csv

Example::

    import cytominer_database.ingest

    cytominer_database.ingest.seed(source, target, config)
"""

import os
import csv
import click
import warnings
import zlib

import pandas as pd
import backports.tempfile
import sqlalchemy.exc
from sqlalchemy import create_engine

import cytominer_database.utils


def __format__(name, header):
    if header in ["ImageNumber", "ObjectNumber"]:
        return header

    return "{}_{}".format(name, header)


def into(input, output, name, identifier, skip_table_prefix=False):
    """Ingest a CSV file into a table in a database.

    :param input: Input CSV file.
    :param output: Connection string for the database.
    :param name: Table in database into which the CSV file will be ingested
    :param identifier: Unique identifier for ``input``.
    :param skip_table_prefix: True if the prefix of the table name should be excluded
     from the names of columns.
    """

    with warnings.catch_warnings():
        # Suppress the following warning on Python 3:
        #
        #   /usr/local/lib/python3.6/site-packages/odo/utils.py:128: DeprecationWarning: inspect.getargspec() is
        #     deprecated, use inspect.signature() or inspect.getfullargspec()
        warnings.simplefilter("ignore", category=DeprecationWarning)
        engine = create_engine(output)
        con = engine.connect()

        df = pd.read_csv(input)
        #print(df['Index'])
        # add "name"prefix to column headers
        if not skip_table_prefix:
            no_prefix = ["ImageNumber", "ObjectNumber"]  # exception columns
            df.columns = [
                i if i in no_prefix else name + "_" + i for i in df.columns
            ]
        # add TableNumber
        number_of_rows, _ = df.shape
        table_number_column = [identifier] * number_of_rows  # create additional column
        df.insert(0, "TableNumber", table_number_column, allow_duplicates=False)
        print("In into(): df.shape is ", df.shape)
        df.to_sql(name=name, con=con, if_exists="append", index = False)

def checksum(pathname, buffer_size=65536):
    """
    Generate a 32-bit unique identifier for a file.

    :param buffer_size: buffer size
    """
    with open(pathname, "rb") as stream:
        result = zlib.crc32(bytes(0))

        while True:
            buffer = stream.read(buffer_size)

            if not buffer:
                break

            result = zlib.crc32(buffer, result)

    return result & 0xffffffff

def seed(source, target, config_path, skip_image_prefix=True):
    """
    Read CSV files into a database backend.

    :param config_file: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Connection string for the database.
    :param  : True if the prefix of image table name should be excluded
     from the names of columns from per image table
    """
    config_file = cytominer_database.utils.read_config(config_path)

    # list the subdirectories that contain CSV files
    directories = sorted(list(cytominer_database.utils.find_directories(source)))

    for directory in directories:
        # get the image CSV and the CSVs for each of the compartments
        try:
            compartments, image = cytominer_database.utils.validate_csv_set(config_file, directory)
        except IOError as e:
            click.echo(e)
            continue
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)
            continue

        # get a unique identifier for the image CSV. This will later be used as the TableNumber column
        # the casting to int is to allow the database to be readable by CellProfiler Analyst, which
        # requires TableNumber to be an integer.
        identifier = checksum(image)
        name, _ = os.path.splitext(config_file["filenames"]["image"])

        # ingest the image CSV
        try:
            into(input=image, output=target, name=name.capitalize(), identifier=identifier,
                 skip_table_prefix=skip_image_prefix)
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)
            continue

        # ingest the CSV for each compartment
        for compartment in compartments:
            name, _ = os.path.splitext(os.path.basename(compartment))

            into(input=compartment, output=target, name=name.capitalize(), identifier=identifier)
