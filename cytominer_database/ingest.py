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

import csv
import hashlib
import os
import warnings

import backports.tempfile
import click
import odo
import sqlalchemy.exc

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

    with backports.tempfile.TemporaryDirectory() as directory:
        source = os.path.join(directory, os.path.basename(input))

        with open(input, "r") as fin, open(source, "w") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            headers = next(reader)
            if not skip_table_prefix:
                headers = [__format__(name, header) for header in headers]
            headers = ["TableNumber"] + headers

            writer.writerow(headers)

            [writer.writerow([identifier] + row) for row in reader]

        with warnings.catch_warnings():
            # Suppress the following warning on Python 3:
            #
            #   /usr/local/lib/python3.6/site-packages/odo/utils.py:128: DeprecationWarning: inspect.getargspec() is
            #     deprecated, use inspect.signature() or inspect.getfullargspec()
            warnings.simplefilter("ignore", category=DeprecationWarning)

            odo.odo(source, "{}::{}".format(output, name), has_header=True, delimiter=",")


def seed(source, target, config_file, skip_image_prefix=True):
    """
    Read CSV files into a database backend.

    :param config_file: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Connection string for the database.
    :param skip_image_prefix: True if the prefix of image table name should be excluded
     from the names of columns from per image table
    """
    config_file = cytominer_database.utils.read_config(config_file)

    directories = sorted(list(cytominer_database.utils.find_directories(source)))

    for directory in directories:
        try:
            patterns, image = cytominer_database.utils.validate_csv_set(config_file, directory)
        except IOError as e:
            click.echo(e)

            continue

        with open(image, "rb") as document:
            identifier = hashlib.md5(document.read()).hexdigest()

        name, _ = os.path.splitext(config_file["filenames"]["image"])

        try:
            into(input=image, output=target, name=name.capitalize(), identifier=identifier,
                 skip_table_prefix=skip_image_prefix)
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)

            continue

        for pattern in patterns:
            name, _ = os.path.splitext(os.path.basename(pattern))

            into(input=pattern, output=target, name=name.capitalize(), identifier=identifier)
