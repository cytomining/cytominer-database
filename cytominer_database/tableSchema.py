import os
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
# reference data ("writer dictionary"), from which the table schemata will be generated.
# The reference tables can either be loaded directly from a designated folder,
# or sampled across all available data, as specified in the config_file.
#
# Also contains helper functions "get_dict_of_pathlist()" and "get_dict_of_paths()"
# to generate a list of paths
# and the sampling function get_reference_paths().
################################################################################



def open_writers(source, target, config_file, skip_image_prefix=True):
    """
    Gets reference tables and uses them to open ParquetWriters.
    Returns a dictionary containing the writers.
    :param source: path to directory containing all parent folders of .csv files
    :target: output file path
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    """
    writers_dict = {}  # nested dictionary: dict in dict
    engine = config_file["database_engine"]["database"]
    reference = config_file["schema"]["reference_option"]

    print("engine = ", engine)
    print("reference =", reference)
    if engine == "SQLite":
        return writers_dict

    if reference == "sample":
        print("open_writers: reference == 'sample' ")
        # we sample from all tables contained in the subdirectories of source
        ref_fraction = float(config_file["schema"]["ref_fraction"])
        print("ref_fraction", ref_fraction)
        # get reference directories for all table kinds, as stored in the dictionary
        path_dict = get_dict_of_pathlist(source=source, directories=None)
        print("path_dict", path_dict)
        ref_dir = get_reference_paths(ref_fraction, path_dict)
        print("ref_dir", ref_dir)

    else: # elif os.path.isdir(os.path.join(source, reference))
        print("reference == ", reference)
        print("open_writers: reference != 'sample' ")
        #'reference' is a path to the folder containing all reference tables (no sampling)
        reference_folder = os.path.join(source, reference)
        assert os.path.isdir(reference_folder)
        ref_dir = get_dict_of_paths(
            reference_folder
        )  # returns values as single string in a dict
        print("------------- in open_writers(): --------------")
        print("ref_dir", ref_dir)
    refIdentifier = 999 & 0xFFFFFFFF
    # arbitrary identifier, will not be stored but used only as type template. (uint32 as in checksum())
    # Iterate over all table kinds:
    for name, path in ref_dir.items():  # iterates over keys of the dictionary ref_dir
        print("name", name)
        print("path", path)
        # unpack path from [path]
        if isinstance(path, list):
            path = path[0]
        print(
            ">>>>>>>>>>>>>>>> In open_writers(): ", name, " <<<<<<<<<<<<<<<<<<<<<<<<,"
        )
        if name == "Image":
            ref_df = cytominer_database.load.get_df(
                input=path,
                identifier=refIdentifier,
                skip_table_prefix=skip_image_prefix,
            )
        else:
            ref_df = cytominer_database.load.get_df(input=path, identifier=refIdentifier)
        # ---------------------- temporary -------------------------------------
        #refPyTable_before_conversion = pyarrow.Table.from_pandas(ref_df)
        #ref_schema_before_conversion = refPyTable_before_conversion.schema[0]
        print("------ In open_writers(): ref_schema_before_conversion --------")
        # print(ref_schema_before_conversion)
        # ----------------------------------------------------------------------
        type_conversion = config_file["schema"]["type_conversion"]
        print(
            "------ In open_writers(): type_conversion: ", type_conversion, "--------"
        )
        if type_conversion == "int2float":
            ref_df = cytominer_database.utils.convert_cols_int2float(
                ref_df
            )  # converts all columns int -> float (except for "TableNumber")
            print("------ converted ref_pandas_df to float --------")
        elif type_conversion == "all2string":
            ref_df = cytominer_database.utils.convert_cols_2string(ref_df)
            print("------ converted ref_pandas_df to string --------")

        ref_table = pyarrow.Table.from_pandas(
            ref_df
        )
        ref_schema = ref_table.schema
        # print("------ In open_writers(): ref_schema (after conversion)")
        # ref_schema_after_conversion = ref_schema[0]
        # print(ref_schema_after_conversion)
        destination = os.path.join(
            target, name + ".parquet"
        )
        writers_dict[name] = {}
        writers_dict[name]["writer"] = pq.ParquetWriter(
            destination, ref_schema, flavor={"spark"}
        )
        writers_dict[name]["schema"] = ref_schema
        writers_dict[name]["pandas_dataframe"] = ref_df
    return writers_dict



def get_dict_of_pathlist(directories=None, source=None):
    """
    Returns a dictionary holding separate lists of all full paths of a table kind.
    (with key: name (table kind), value = list).
    # each containing of all of its existing full table paths.
    :directories: list of (sub)directories which hold .csv files.
    Example: ["path/plate_a/set_1" , "path/plate_a/set_2"]
    :source: path to directories containing subdirectories.
    Example: "path/plate_a"
    """
    # Notes:
    # - designed to take *either* a list of directories *or* a single source path.
    # - The argument "directories" allows to get all table paths from all directories in the list.
    #   This was added as a help to test the function for corner cases. Can be removed.
    #   Then the case distinction at the beginning can be removed as well!
    # - The argument "source" specifies the parent directory, from which all
    #  subdirectories will be read and sorted and used to get all full paths to
    #  all tables, separately for each table kind. This argument is used when
    #  the function is called from "open_writers()"", for the option 'sampling'.
    # - The function returns a dictionary to "open_writers()"",
    #   which is passed to "get_reference_paths()",
    #   which samples paths from the lists and selects the reference table among them.
    #- Attention: returns a dictionary with value = list, even if there is only a single element
    #- We could include the old csv-validation (cytominer_database.utils.validate_csv_set(config_file, directory))

    # initialize dictionary that will be returned
    return_dict = {}
    # case 1 : no arguments passed ---> Error
    if source is None and directories is None:
        print("Error: No arguments passed to get_dict_of_pathlist()")
        return None
    # case 2: Only "source"  was passed (and serves as parent directory)
    # --> get list of subdirectories.
    elif not directories:
        directories = sorted(list(cytominer_database.utils.find_directories(source)))
    # case 3 : list of specified folders was passed and can be used directly.

    # iterate over all (sub)directories.
    for directory in directories:
        # Get the names of all files in that directory (e.g. Cells.csv)
        filenames = os.listdir(directory)
        for filename in filenames:
            # get full path
            fullpath = os.path.join(directory, filename)
            # get table kind name without .csv extension
            name, _ = os.path.splitext(filename)
            # standardize
            name = name.capitalize()
            # initialize dictionary entry if it does not exist yet
            if name not in return_dict.keys():
                return_dict[name] = []
            # add path to list under the entry of its table kind
            return_dict[name] += [fullpath]
    return return_dict


def get_dict_of_paths(folder):
    """
    Returns a dictionary with key: name (table kind), value = path string.
    :folder: path to folder that contains tables
    Example: "path/plate_a/special_set"
    """
    # Note: similar to get_dict_of_pathlist(source, directories=None),
    # Difference: Dictionary values are a single table path for each table kind
    # that is contained in a single directory "folder".
    # Does not accept a list of directories as an input
    # and does not return a list of all table paths under a source directory.
    # We could include the old csv-validation (cytominer_database.utils.validate_csv_set(config_file, directory))

    return_dict = {}
    filenames = os.listdir(folder)
    for filename in filenames:
        name, _ = os.path.splitext(filename)  # filename contains .csv extension
        name    = name.capitalize()
        # print("name = ",  name)
        fullpath = os.path.join(folder, filename)
        return_dict[name] = fullpath  # note that the path can be overwritten here.
    return return_dict

def get_reference_paths(ref_fraction, full_paths):
    """
    Samples a subset of all existing full paths and determines the reference table among them.
    Returns a dictionary with key: name (table kind), value = full path to reference table.
    :ref_fraction: fraction of all paths to be compared (relative sample set size).
    :full_paths: dictionary containing a list of all full table paths for each table kind
    Example: {Image: [path/plate_a/set_1/image.csv, path/plate_a/set_2/image.csv,... ], Cells: [path/plate_a/set_1/Cells.csv, ...], ...}
    """
    print(" ------------------- Entering get_reference_paths() -------------------")
    # Note: returns full paths, not parent directories
    # -------------------------------------------------
    #  - samples only among directories in which that table kind exists.
    #  - sample by taking the first n element after permuting the list elements at random
    #  - option to pass a custom list of directories among which to sample
    #     (default is None, then the list of all existing directories is derived from source)
    # -------------------------------------------------
    # Note: if directories are passed
    # 0. initialize return dictionary
    ref_dirs = {}
    # print("full_paths: " , full_paths)
    # 1. iterate over table types
    for key, value in full_paths.items():  # iterate over table types
        # print("--------------------- In get_reference_paths(): Getting ref for table type : ", key, "---------------------")
        # 2. Permute the table list at random
        # print("dict value : ", np.random.shuffle(value))
        np.random.shuffle(value)  # alternative: assign to new list
        # print("Shuffled dict values : value = ", value)
        # 3. get first n items corresponding to fraction of files to be tested (among the number of all tables present for that table kind)
        # --------------------------- constants ----------------------------------
        sample_size = int(np.ceil(ref_fraction * len(value)))
        # print("sample_size", str(sample_size))
        # --------------------------- variables ----------------------------------
        max_width = 0
        # ------------------------------------------------------------------------
        for path in value[
            :sample_size
        ]:  # iterate over random selection of tables of that type
            # read first row of dataframe
            # check if path leads to a valid, non-empty file
            # if cytominer_database.utils.validate_csv(path) == "No errors.": ---> does not work
            if cytominer_database.utils.validate_csv(path):
                df_row = pd.read_csv(path, nrows=1)
                # print("df_row", df_row)
                # print("df_row.shape[1]", df_row.shape[1])
                # check if it beats current best (widest table)
                if df_row.shape[1] > max_width:  # note: .shape returns [length, width]
                    # update
                    # print("updated max_width = ", str(max_width))
                    max_width = df_row.shape[1]
                    # print("to max_width = ", str(max_width))
                    ref_dirs[key] = path
            elif sample_size < len(value) : # invalid file, but not all files were sampled yet
                sample_size += 1 # get a substitute sample file
    print(" ------------------- Leaving get_reference_paths() -------------------")
    return ref_dirs
