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

    return [
        os.path.join(directory, x)
        for x in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, x))
        ]


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

    :return: a tuple where the first element is the list of compartment CSV files, the second is the image CSV file.

    """

    # get the image CSV
    image_csv = os.path.join(directory, config["filenames"]["image"])

    if not os.path.isfile(image_csv):
        raise IOError("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))

    # get the CSV file for each compartment
    compartment_csvs = collect_csvs(config, directory)

    filenames = compartment_csvs + [image_csv]

    # validate all the CSVs
    file_checks = dict({(filename, validate_csv(filename)) for filename in filenames})

    # if any CSV is invalid, throw an error
    if not all(file_checks.values()):
        invalid_files = ",".join([os.path.basename(filename) for (filename, valid) in file_checks.items() if not valid])
        raise IOError("Some files were invalid: {}. Skipping {}.".format(invalid_files, directory))

    return compartment_csvs, image_csv


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
    Read a configuration file. A default config file is read first, and the values are overriden
    by those in the specified configuration file.

    :param filename: configuration filename

    :return: a configuration object
    """
    config = configparser.ConfigParser()

    for config_filename in [

        pkg_resources.resource_filename("cytominer_database", "config/config_default.ini"),  # get default config file
        filename
    ]:
        try:
            with open(config_filename, "r") as fd:
                config.read_file(fd)
        except IOError as e:
            logger.warning("Unable to read configuration file: {}.".format(config_filename))

    return config


def type_convert_dataframe(dataframe, config_file):
    """
    Type casting of entire pandas dataframe.
    Calls conversion function based on specifications in configuration file.
    :param dataframe: input file
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))
    """
    engine = config_file["ingestion_engine"]["engine"]
    if engine == "Parquet": # convert. (Else: do nothing.)
        type_conversion = config_file["schema"]["type_conversion"]
        if type_conversion == "int2float":
            convert_cols_int2float(dataframe)
        elif type_conversion == "all2string":
            convert_cols_2string(dataframe)
        else: 



def convert_cols_int2float(pandas_df):
    """
    Converts all columns with type 'int' to 'float'.
    :pandas_df: Pandas dataframe
    """

    # iterates over the columns of the input Pandas dataframe
    # and converts int-types to float.
    for i in range(len(pandas_df.columns)):
        if pandas_df.dtypes[i] == "int":
            name = pandas_df.columns[i]  # column headers
            # Strict int-type columns: Do not convert these columns from int to float
            keep_int = ["ImageNumber", "ObjectNumber", "TableNumber"]
            if name in keep_int:
                continue
            pandas_df[name] = pandas_df[name].astype("float")



def convert_cols_2string(dataframe):
    """
    Converts all column types to 'string'.
    :dataframe: Pandas dataframe
    """
    # iterates over the columns of the input Pandas dataframe
    # and converts all values to type string
    for col_name in dataframe.columns:
        dataframe[col_name] = dataframe[col_name].astype("str")


def get_name(file_path):
    """
    Returns the capitalized basename of a file path (without the file extension).

    :file_path: Pandas dataframe
    """
    name, _ = os.path.splitext(os.path.basename(file_path))
    return name.capitalize()

