"""
"""

import glob
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

def find_directories(directory):
    """List subdirectories.

    :param directory: directory

    :return: list of subdirectories of ``directory``

    """
    directories = set()

    filenames = glob.glob(os.path.join(directory, '*/'))

    for filename in filenames:
        pathname = os.path.relpath(filename)

        directories.add(pathname)

    return directories


def validate_csv(csvfile):
    """Validate CSV file.

    The CSV file typically corresponds to either a measurement made on a compartment, e.g. Cells.csv, or on an image,
    e.g. Image.csv. The validation performed is generic - it simply checks for malformed CSV files.

    This uses csvclean to check for validity of a CSV file.

    :param csvfile: CSV file to validate

    :return: True if valid, False otherwise.

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
    """Validate a set of CSV files.

    This function validates a set of CSV files in a directory. These CSV files correspond to measurements
    made on different cellular compartments, e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv. An Image.csv file,
    corresponding to measurements made on the whole image, along with metadata, is also typically present.

    :param config: configuration file - this contains the set of CSV files to validate.
    :param directory: directory containing the CSV files.

    :return: a tuple where the first element is the list of pattern CSV files, the second is the image CSV file.

    """
    image_csv = os.path.join(directory, config["filenames"]["image"])

    if not os.path.isfile(image_csv):
        raise OSError("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))

    pattern_csvs = [filename for filename in glob.glob(os.path.join(directory, '*.csv')) if filename not in [
        os.path.join(directory, config['filenames']['image']),
        os.path.join(directory, config['filenames']['object']),
        os.path.join(directory, config['filenames']['experiment'])
    ]]

    filenames = pattern_csvs + [image_csv]

    file_checks = dict({(filename, validate_csv(filename)) for filename in filenames})

    if not all(file_checks.values()):
        invalid_files = ",".join([os.path.basename(filename) for (filename, valid) in file_checks.items() if not valid])
        raise OSError("Some files were invalid: {}. Skipping {}.".format(invalid_files, directory))

    return pattern_csvs, image_csv
