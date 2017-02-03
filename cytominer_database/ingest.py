"""

"""

import click
import configparser
import csv
import hashlib
import odo
import os
import cytominer_database.utils
import pkg_resources
import subprocess
import tempfile
import sqlalchemy

def __format__(name, header):
    if header in ["ImageNumber", "ObjectNumber"]:
        return header

    return "{}_{}".format(name, header)


def into(input, output, name, identifier):
    """

    :param input:
    :param output:
    :param name:
    :param identifier:

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

    :param config:
    :param source:
    :param target:

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
    version=pkg_resources.get_distribution("cytominer_database").version
)
def __main__(config_file, source, target, munge):
    """

    :param config_file:
    :param source:
    :param target:
    :param munge:

    :return:

    """

    if munge:
        subprocess.call([pkg_resources.resource_filename(__name__, os.path.join("scripts", "munge.sh")), source])

    config = configparser.ConfigParser()

    config.read(config_file)

    seed(source, target, config)

if __name__ == "__main__":
    __main__()