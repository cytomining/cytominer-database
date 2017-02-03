"""
"""

import glob
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

def find_directories(directory):
    """

    :param directory:

    :return:

    """
    directories = set()

    filenames = glob.glob(os.path.join(directory, '*/'))

    for filename in filenames:
        pathname = os.path.relpath(filename)

        directories.add(pathname)

    return directories


def validate_csv(csvfile):
    """

    :param csvfile:

    :return:

    """
    cmd = "csvclean -n {}".format(csvfile)

    try:
        csvcheck = subprocess.check_output(cmd, shell=True)

    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            return False

        elif e.returncode == 127:
            logger.warning("csvclean not found. Validation will not be rigorous.")
            
        else:
            raise subprocess.CalledProcessError(e.returncode, e.cmd)

    nrows = sum(1 for line in open(csvfile)) - 1

    file_size = os.stat(csvfile).st_size

    return (file_size > 0) & (nrows >= 1) & (csvcheck == b'No errors.\n')


def validate_csv_set(config, directory):
    """

    :param config:
    :param directory:

    :return:

    """
    image_csv = os.path.join(directory, config["filenames"]["image"])

    if not os.path.isfile(image_csv):
        raise OSError("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))

    pattern_csvs = [filename for filename in glob.glob(os.path.join(directory, '*.csv')) if filename not in [
        os.path.join(directory, config['filenames']['image']),
        os.path.join(directory, config['filenames']['object']),
        os.path.join(directory, config['filenames']['experiment'])
    ]]

    file_checks = dict({(filename, validate_csv(filename)) for filename in [*pattern_csvs, image_csv]})

    if not all(file_checks.values()):
        invalid_files = ",".join([os.path.basename(filename) for (filename, valid) in file_checks.items() if not valid])
        raise OSError("Some files were invalid: {}. Skipping {}.".format(invalid_files, directory))

    return pattern_csvs, image_csv
