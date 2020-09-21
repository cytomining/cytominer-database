import os.path
import csv
import click
import warnings
import zlib
import pandas as pd
import backports.tempfile
import sqlalchemy.exc
from sqlalchemy import create_engine
import pyarrow
import pyarrow.parquet as pq
import pyarrow.csv
import numpy as np
import collections
import numpy as np
import cytominer_database
import cytominer_database.utils
import cytominer_database.load

################################################################################
# Contains "open_writers()"" to generate the dictionary containing the
#  reference data ("writer dictionary"), from which the table schemata will be generated.
# The reference tables can either be loaded directly from a designated folder,
# or sampled across all available data, as specified in the config_file.
#
# Also contains helper functions "get_table_paths()" to generate a list of paths
# and the sampling function get_reference_paths().
################################################################################


def open_writers(source, target, config_file, engine, skip_image_prefix=True):
    """
    Determines, loads reference tables and openes them as ParquetWriters.
    Returns a dictionary referencing the writers.
    :param source: path to directory containing all parent folders of .csv files
    :target: output file path
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))
    :engine: specifies backend ('SQLite' or 'Parquet')
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    :writers_dict: dictionary referencing the writers (return argument)
    """
    if engine == "SQLite":  # no reference table needed
        return None

    reference_directories = get_path_dictionary(
        config_file, source
    )  # includes different steps, depending on config_file
    writers_dict = {}
    refIdentifier = 999 & 0xFFFFFFFF
    # arbitrary identifier, will not be stored but used only as type template. (uint32 as in checksum())
    # Iterate over all table kinds:
    for (
        name,
        path,
    ) in (
        reference_directories.items()
    ):  # iterates over keys of the dictionary reference_directories
        # unpack path from [path] # if isinstance(path, list):
        path = path[0]
        # load dataframe. Attention: Unpack from list return argument
        ref_df = cytominer_database.load.get_and_modify_df(
            path, refIdentifier, skip_image_prefix
        )
        #  is also used in ingest.seed()
        cytominer_database.utils.type_convert_dataframe(ref_df, engine, config_file)
        ref_table = pyarrow.Table.from_pandas(ref_df)
        ref_schema = ref_table.schema
        destination = os.path.join(target, name + ".parquet")
        writers_dict[name] = {}
        writers_dict[name]["writer"] = pq.ParquetWriter(
            destination, ref_schema, flavor={"spark"}
        )
        writers_dict[name]["schema"] = ref_schema
        writers_dict[name]["pandas_dataframe"] = ref_df
    return writers_dict


def get_path_dictionary(config, source):
    """
    Determines a single reference directory for every table kind and 
    returns a dictionary with key: 'Capitalized_table_kind', value = 'full/path/to/reference_table.csv'

    :param config: parsed configuration data (output from cytominer_database.utils.read_config(config_path))
    :param source: path to directory containing all parent folders of .csv files
    """
    reference = config["schema"]["reference_option"]

    if (
        reference != "sample"
    ):  # reference tables are given in folder with full path os.path.join(source, reference)
        if os.path.isdir(os.path.join(source, reference)):
            #'reference' is a path to the folder containing all reference tables (no sampling)
            # get_dict_of_paths() returns values as single string in a dict
            directory = [os.path.join(source, reference)]  #  note: input is a list
            path_dictionary = directory_list_to_path_dictionary(directory)
        else:
            warnings.warn(
                "{} is not a valid path for a reference file directory. The reference tables are sampled instead. Fix this by adjunsting config_file['schema']['reference_option']".format(
                    os.path.join(source, reference)
                ),
                UserWarning,
            )
            reference = "sample"  # proceed with sampling
    if reference == "sample":
        # Idea: sample from all tables contained in the subdirectories of source.
        # Get all possible reference directories for each table kind, as stored in the dictionary.

        # Get sample size (as fraction of all available files) from config file
        ref_fraction = float(config["schema"]["ref_fraction"])
        # Get directories list
        directories = sorted(list(cytominer_database.utils.find_directories(source)))
        # get all full paths stored as lists in a dictionary
        full_paths = directory_list_to_path_dictionary(directories)
        # sample from all paths, determine reference paths, store in dictionary
        path_dictionary = sample_reference_paths(ref_fraction, full_paths)
    return path_dictionary


def directory_list_to_path_dictionary(directories):
    """
    Returns a dictionary holding a list of all full paths (as value) for every table kind (key).
    Example: directories = ["path/plate_a/set_1" , "path/plate_a/set_2"] returns
     grouped_table_paths = {"Cells": ["path/plate_a/set_1" , "path/plate_a/set_2"], ...
     "Cytoplasm": ["path/plate_a/set_1" , "path/plate_a/set_2"]}
     if set_1 and set_2 both contain the files "Cells.csv" and "Cytoplasm.csv" (and no other files).

    :param directories: list of directories in which .csv files are stored, e.g. "path/plate_a/set_1" , "path/plate_a/set_2"].
    :param grouped_table_paths: dictionary containing a list of all full table paths for each table kind
    """
    # Notes:
    # - The argument "source" specifies the parent directory, from which all
    #  subdirectories will be read and sorted and used to get all full paths to
    #  all tables, separately for each table kind. This argument is used when
    #  the function is called from "open_writers()"", for the option 'sampling'.
    # - The function returns a dictionary to "open_writers()"". It is then passed to "get_reference_paths()",
    #   which samples paths from the lists and selects the reference table among them.
    # - Attention: returns a dictionary with value = list, even if there is only a single element
    # - Option: We could include the old csv-validation (cytominer_database.utils.validate_csv_set(config_file, directory))

    # initialize dictionary that will be returned
    table_paths = {}
    # iterate over all (sub)directories.
    for directory in directories:
        # Get all filenames in that directory (e.g. 'Cells.csv')
        filenames = os.listdir(directory)
        for filename in filenames:
            # get name (w/o extension, e.g. 'Cells') and full path (e.g. 'path/to/Cells.csv')
            name, _ = os.path.splitext(filename)
            name = name.capitalize()
            fullpath = os.path.join(directory, filename)
            # initialize dictionary entry if it does not exist yet
            if name not in table_paths.keys():
                table_paths[name] = []
            # extend the list of paths by current path
            table_paths[name] += [fullpath]
    return table_paths


def sample_reference_paths(ref_fraction, full_paths):
    """
    Samples a subset of all existing full paths and determines the reference table among them.
    Returns a dictionary with key: name (table kind), value = full path to reference table.
    :param ref_fraction: fraction of all paths to be compared (relative sample set size).
    :param source:
    
    :full_paths: dictionary containing a list of all full table paths for each table kind
    Example: {Image: [path/plate_a/set_1/image.csv, path/plate_a/set_2/image.csv,... ], Cells: [path/plate_a/set_1/Cells.csv, ...], ...}
    """
    # Note: returns full paths (not parent directories)
    # -------------------------------------------------
    #  - samples only among directories in which that table kind exists.
    #  - sample by taking the first n elements after permuting the list elements at random
    # -------------------------------------------------

    # 0. initialize return dictionary
    sampled_path_dictionary = {}
    # 1. iterate over table types
    for filename, filepath in full_paths.items():  # iterate over table types
        # 2. Permute the table list at random
        np.random.shuffle(filepath)
        # 3. get first n items corresponding to fraction of files to be tested (among the number of all tables present for that table kind)
        # --------------------------- constants ----------------------------------
        sample_size = int(np.ceil(ref_fraction * len(filepath)))
        # --------------------------- variables ----------------------------------
        max_width = 0
        # ------------------------------------------------------------------------
        for path in filepath[
            :sample_size
        ]:  # iterate over random selection of tables of that type
            # read first row of dataframe
            # check if path leads to a valid, non-empty file
            # if cytominer_database.utils.validate_csv(path) == "No errors.": ---> does not work
            if cytominer_database.utils.validate_csv(path):
                df_row = pd.read_csv(path, nrows=1)
                # check if it beats current best (widest table)
                if df_row.shape[1] > max_width:  # note: .shape returns [length, width]
                    # update
                    max_width = df_row.shape[1]
                    sampled_path_dictionary[filename] = [
                        path
                    ]  # Note: Need list as dictionary value (to unify next steps)
            elif sample_size < len(
                filepath
            ):  #  invalid file, but not all files were sampled yet
                sample_size += 1  # get a substitute sample file
            else:
                warnings.warn(
                    " Not enough valid .csv files to compare the fraction={} of all .csv files (reference file sampling).".format(
                        ref_fraction
                    ),
                    UserWarning,
                )
    return sampled_path_dictionary
