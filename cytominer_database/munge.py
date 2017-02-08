"""

"""

import click
import cytominer_database.utils
import os
import pandas


def munge(config, source, target=None):
    """

    :param config: Configuration file.
    :param source: Directory containing subdirectories that contain an object CSV file.
    :param target: Output directory. If not specified, then it is same as``source``.

    :return: list of subdirectories that have an object CSV file.

    """
    directories = sorted(list(cytominer_database.utils.find_directories(source)))

    valid_directories = [] # list of subdirectories that have an object CSV file.

    for directory in directories:
        try:
            obj = pandas.read_csv(os.path.join(directory, config["filenames"]["object"]), header=[0, 1])

        except IOError as e:
            click.echo(e)

            continue

        valid_directories.append(directory)

        target_directory = directory.replace(source, target)

        os.mkdir(target_directory)

        for compartment_name in set(obj.columns.get_level_values(0).tolist()) - {'Image'}:

            # select columns of the compartment
            compartment = pandas.concat([obj['Image'], obj[compartment_name]], axis=1)

            # Create a new column
            compartment['ObjectNumber'] = compartment['Number_Object_Number']

            cols = compartment.columns.tolist()

            # Move ImageNumber and ObjectNumber to the front

            cols.insert(0, cols.pop(cols.index('ObjectNumber')))

            cols.insert(0, cols.pop(cols.index('ImageNumber')))

            compartment = compartment.ix[:, cols]

            compartment.to_csv(os.path.join(target_directory, compartment_name + '.csv'), index=False)

    return valid_directories
