"""

"""

import click
import cytominer_database.utils
import os
import pandas


def munge(config, source, target=None):
    """

    :param config: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Output directory. If not specified, then it is same as``source``.

    :return:

    """
    directories = sorted(list(cytominer_database.utils.find_directories(source)))

    for directory in directories:
        try:
            obj = pandas.read_csv(os.path.join(directory, config["filenames"]["object"]), header=[0, 1])

        except IOError as e:
            click.echo(e)

            continue

        target_directory = directory.replace(source, target)

        os.mkdir(target_directory)

        for compartment_name in set(obj.columns.get_level_values(0).tolist()) - {'Image'}:

            compartment = pandas.concat([obj['Image'], obj[compartment_name].rename(
                columns=lambda x: (compartment_name + '_' + x) if x != 'ObjectNumber' else x)], axis=1)

            compartment['ObjectNumber'] = compartment[compartment_name + '_' + 'Number_Object_Number']

            cols = compartment.columns.tolist()

            cols.insert(0, cols.pop(cols.index('ObjectNumber')))

            cols.insert(0, cols.pop(cols.index('ImageNumber')))

            compartment = compartment.ix[:, cols]

            compartment.to_csv(os.path.join(target_directory, compartment_name + '.csv'), index=False)