import csv
import glob
import logging
import os
import pkg_resources
import tempfile
import warnings

import configparser
import csvkit.utilities.csvclean

# csvkit (or a dependency of csvkit) mucks with warning levels.
# reset warnings to default after importing csvkit.
warnings.resetwarnings()

logger = logging.getLogger(__name__)


def find_directories(directory):
    """
    List subdirectories.

    :param directory: directory

    :return: list of subdirectories of ``directory``

    """
    return glob.glob(os.path.join(directory, '*/'))


def validate_csv(csvfile):
    """
    Validate a CSV file.

    The CSV file typically corresponds to either a measurement made on a compartment, e.g. Cells.csv, or on an image,
    e.g. Image.csv. The validation performed is generic - it simply checks for malformed CSV files.

    This uses csvclean to check for validity of a CSV file.

    :param csvfile: CSV file to validate

    :return: True if valid, False otherwise.

    """
    if os.stat(csvfile).st_size == 0:
        return False

    with open(csvfile, "r") as csvfd:
        reader = csv.reader(csvfd)

        nrows = sum(1 for _ in reader) - 1  # exclude header

    if nrows < 1:
        return False

    with tempfile.TemporaryFile(mode="w+") as tmpfile:
        csvclean = csvkit.utilities.csvclean.CSVClean(args=["-n", csvfile], output_file=tmpfile)

        csvclean.run()

        tmpfile.seek(0)

        csvcheck = tmpfile.read().strip()

    return csvcheck == "No errors."


def validate_csv_set(config, directory):
    """
    Validate a set of CSV files.

    This function validates a set of CSV files in a directory. These CSV files correspond to measurements
    made on different cellular compartments, e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv. An Image.csv file,
    corresponding to measurements made on the whole image, along with metadata, is also typically present.

    :param config: configuration file - this contains the set of CSV files to validate.
    :param directory: directory containing the CSV files.

    :return: a tuple where the first element is the list of pattern CSV files, the second is the image CSV file.

    """
    image_csv = os.path.join(directory, config["filenames"]["image"])

    if not os.path.isfile(image_csv):
        raise IOError("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))

    pattern_csvs = collect_csvs(config, directory)

    filenames = pattern_csvs + [image_csv]

    file_checks = dict({(filename, validate_csv(filename)) for filename in filenames})

    if not all(file_checks.values()):
        invalid_files = ",".join([os.path.basename(filename) for (filename, valid) in file_checks.items() if not valid])
        raise IOError("Some files were invalid: {}. Skipping {}.".format(invalid_files, directory))

    return pattern_csvs, image_csv


def collect_csvs(config, directory):
    """
    Collect CSV files from a directory.

    This function collects CSV files in a directory, excluding those that have been specified in the configuration file.
    This enables collecting only those CSV files that correspond to cellular compartments. e.g. Cells.csv, Cytoplasm.csv,
    Nuclei.csv. CSV files corresponding to experiment, image, or object will be excluded.

    :param config: configuration file.
    :param directory: directory containing the CSV files.

    :return: a list of CSV files.

    """
    config_filenames = []

    for filename_option in ["experiment", "image", "object"]:
        if config.has_option("filenames", filename_option):
            config_filenames.append(os.path.join(directory, config["filenames"][filename_option]))

    filenames = glob.glob(os.path.join(directory, "*.csv"))

    return [filename for filename in filenames if filename not in config_filenames]


def read_config(filename):
    """
    Read a configuration file.

    :param filename: configuration filename

    :return: a configuration object
    """
    config = configparser.ConfigParser()

    for config_filename in [
        pkg_resources.resource_filename("cytominer_database", "config/config_default.ini"),  # default config file
        filename
    ]:
        try:
            with open(config_filename, "r") as fd:
                config.read_file(fd)
        except IOError as e:
            logger.warn("Unable to read configuration file: {}.".format(config_filename))

    return config
