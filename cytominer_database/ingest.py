"""``ingest`` - mechanism to ingest CSV files into a database.

    In morphological profiling experiments, a CellProfiler pipeline is often run in parallel across multiple images and
    produces a set of CSV files. For example, imaging a 384-well plate, with 9 sites per well, produces 384 * 9 images;
    a CellProfiler process may be run on each image, resulting in a 384*9 output directories (each directory typically
    contains one CSV file per compartment (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv) and one CSV file for per-image
    measurements (e.g. Image.csv).

    ``ingest`` can be used to read all these CSV files into a database backend. SQLite is the recommended engine, but ingest
    will likely also work with PostgreSQL and MySQL.

    ``ingest`` assumes a directory structure like shown below:

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

    :Example:

        ``$ ingest plate_a -o sqlite:///plate_a.sqlite -c ingest_config.ini``


"""

import click
import configparser
import csv
import hashlib
import odo
import os
import cytominer_database.utils
import cytominer_database.munge
import pkg_resources
import tempfile
import sqlalchemy

def __format__(name, header):
    if header in ["ImageNumber", "ObjectNumber"]:
        return header

    return "{}_{}".format(name, header)


def into(input, output, name, identifier):
    """Ingest a CSV file into a table in a database.

    :param input: Input CSV file.
    :param output: Connection string for the database.
    :param name: Table in database into which the CSV file will be ingested
    :param identifier: Unique identifier for ``input``.

    :return:

    """

    with tempfile.TemporaryDirectory() as directory:
        source = os.path.join(directory, os.path.basename(input))

        with open(input, "r") as fin, open(source, "w") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            headers = next(reader)
            headers = [__format__(name, header) for header in headers]
            headers = ["TableNumber"] + headers

            writer.writerow(headers)

            [writer.writerow([identifier] + row) for row in reader]

        odo.odo(source, "{}::{}".format(output, name), has_header=True, delimiter=",")


def seed(source, target, config):
    """
    Call functions to create backend

    :param config: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Connection string for the database.

    :return:

    """

    directories = sorted(list(cytominer_database.utils.find_directories(source)))

    for directory in directories:
        try:
            patterns, image = cytominer_database.utils.validate_csv_set(config, directory)
        except IOError as e:
            click.echo(e)

            continue

        with open(image, "rb") as document:
            identifier = hashlib.md5(document.read()).hexdigest()

        name, _ = os.path.splitext(config["filenames"]["image"])

        try:
            into(input=image, output=target, name=name, identifier=identifier)
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)

            continue

        for pattern in patterns:
            name, _ = os.path.splitext(os.path.basename(pattern))

            into(input=pattern, output=target, name=name, identifier=identifier)


@click.command()
@click.argument(
    "source",
    type=click.Path(exists=True)
)
@click.option(
    "-c",
    "--config_file",
    default=pkg_resources.resource_filename(__name__, os.path.join("config", "config_htqc.ini")),
    type=click.Path(exists=True)
)
@click.option(
    "--munge/--no-munge",
    default=True
)
@click.option(
    "-o",
    "--target",
    type=click.Path(writable=True)
)
@click.version_option(
    version=pkg_resources.require("cytominer_database")[0].version
)
def __main__(config_file, source, target, munge):
    """

    :param config_file: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Connection string for the database.
    :param munge: True if the CSV files for individual compartments have been merged into a single CSV file.
    If True, the CSV will be split into one CSV per compartment.

    :return:

    """

    config = configparser.ConfigParser()

    config.read(config_file)

    if munge:
        cytominer_database.munge.munge(config=config, source=source)

    seed(source, target, config)

if __name__ == "__main__":
    __main__()
