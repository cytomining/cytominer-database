"""
A mechanism to ingest CSV files into a database.

In morphological profiling experiments, a CellProfiler pipeline is often run in parallel across multiple images and
produces a set of CSV files. For example, imaging a 384-well plate, with 9 sites per well, produces 384 * 9 images;
a CellProfiler process may be run on each image, resulting in a 384*9 output directories (each directory typically
contains one CSV file per compartment (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv) and one CSV file for per-image
measurements (e.g. Image.csv).

``cytominer_database.ingest.seed`` can be used to read all these CSV files into a database backend. SQLite is the
recommended engine, but ingest will likely also work with PostgreSQL and MySQL.

``cytominer_database.ingest.seed`` assumes a directory structure like shown below:

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

Example::

    import cytominer_database.ingest

    cytominer_database.ingest.seed(source, target, config)

"""

"""
* Include in documentation:
- requirements: csvkit
- parameter options read from config.ini --> see .txt file


* Important notes:

- before opening a writer we need to determine and set a fixed table schema, as given by pyarrow.Table.schema .
We then force all tables to adhere to that schema by
(1) performing the same type conversion
(as selected in the config.ini and explicitly implemented by the conversion functions below, e.g by
converting most column types int -> float (will be a small fraction of columns!))
and by
(2) using the .align method for pandas dataframes on all tables, with respect to the reference table.
We do not allow implicit schema conversion (instead we assure the column headers and types are identical
 before we write to the table!) and we decided against
the inclusion of functions that do automatic type-hierarchy-based casting (preserved as redundant functions).
- We choose the reference table either based on random sampling (specified by a sampling fraction
and carried out on all subfolders in the source folder, e.g. "plate_a/")
or by using the tables in a prespecified folder. This option is selected in the config.ini as well.

- The script contains many print() statements. They can all be deleted.


* Special cases

[DONE]
- missing columns
- type disagreements the same column headers of different sets (same table type, e.g. Cells.csv)
- null values etc in tables that are read in after the correct reference table has been determined

[TODO]
- what happens if by accident the reference table has the wrong type?

"""
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


def write_csv_to_sqlite(input, output, identifier, skip_table_prefix=False):
    """
    Gets a modified dataframe, opens an engine and writes to SQLite

    :param input: input file path
    :param output: output file path
    :param identifier: TableNumber that is added as column. Argument is only passed down.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables,
     if passed explicitly.
    """
 # Note on skip_table_prefix:
 # Per default, header prefixes are not added to image.csv tables.
 # Header prefixes are always added to all other table types.
 # This is why the writing functions "write_csv_to_SQLite/Parquet()" call the
 # "image" separately from "compartments"
 # (in the first case, the argument skip_image_prefix is passed).
 # An alternative would be to remove this case distinction and not pass skip_image
 # as an argument. The option can be read from the config file directly before
 # modifying the headers. This would require passing the config file along,
 # which is already done in many functions, since the ParquetWriter introduces
 # many different options in config.ini.
 # Also note that we use skip_table_prefix and skip_image_prefix interchangebly.

    print("............... in write_csv_to_sqlite:", name, "..................")
    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()
    # get dataframe
    dataframe = get_df_from_temp_dir(input, identifier, skip_table_prefix)
    # Note: we could generate the identifier within get_df_from_temp_dir(),
    # instead of passing it to write_csv_to_sqlite() and get_df_from_temp_dir().
    # However, we need to access the image.csv in the parent directory,
    # which can get messy if we're writing another table kind
    # (e.g.input = ....../Cells.csv ----> access  ....../image.csv)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        engine = create_engine(output)
        con = engine.connect()
        dataframe.to_sql(name=name, con=con, if_exists="append")

def write_csv_to_parquet(
    input, output, identifier, writers_dict, skip_table_prefix=False
):
    """
    Gets a modified dataframe and writes Table to opened ParquetWriter
    :param input: input file path
    :param output: output file path
    :param writers_dict : dictionary of dictionaries for opened ParquetWriters.
    Key: Name ; Values: {"writer" : ....}, {"schema": ...}, {"pandas_dataframe": ...}.
    :param identifier: TableNumber that is added as column before writing the table.
      Note: The paramenter is only passed down.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables, if
     passed explicitly. Note: The parameter is only passed down.
    """
    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()
    print("............... in write_csv_to_pa rquet: ", name, ".................")
    print(input)
    # get dataframe
    dataframe = get_df(input, identifier, skip_table_prefix)
    # --------debugging---------
    # print(dataframe.columns.get_loc("TableNumber")) # should be '0'
    # ----------------------------
    dataframe = type_convert_dataframe(dataframe, config_file)
    ref_dataframe = writers_dict[name]["pandas_dataframe"]
    # ---------alignment---------
    # Note: missing columns are added with same name and type as in ref_dataframe, but containing NaN values.
    dataframe, ref_dataframe_new = dataframe.align(ref_dataframe, join="right", axis=1)
    assert ref_dataframe_new.equals(
        ref_dataframe
    )  # here we assert that the reference table has not been modified by the alignment.
    # Alternatively, we can just do "dataframe,_ = dataframe.align(ref_dataframe, join="right", axis=1)

    # --------- temporary: check agreement of pandas df ------
    # check if the pyarrow schemata will be identical
    assert dataframe.dtypes.equals(ref_dataframe.dtypes)
    # ---------------------------------------------------------
    # read into pyarrow table format
    table = pyarrow.Table.from_pandas(dataframe)
    # --------- temporary: check agreement of pyarrow format ------
    assert table.schema is writers_dict[name]["schema"]
    # ---------------------------------------------------------
    # get opened parquet writer, write table to file
    print(
        "------------",
        name,
        ": in write_csv_to_parquet: writing next table to Parquet file ------------",
    )
    writer = writers_dict[name]["writer"]
    writer.write_table(table)
    print(
        "------------",
        name,
        ": in write_csv_to_parquet: finished writing next table to Parquet file-------------",
    )


def __format__(name, header):
    """
    Adds a prefix to a column header. Returns modified header.
    :param name: string of table name
    :param header: column header
    """
    # exclude specific column headers
    if header in ["TableNumber", "ImageNumber", "ObjectNumber"]:
        return header
    return "{}_{}".format(name, header)


def get_df_from_temp_dir(input, identifier, skip_table_prefix=False):
    """
    Loads .csv file to temporary directory, adds prefix to column headers,
     adds TableNumber column, returns table as pandas dataframe. Based on
      previous code. Used only for SQLite backend.
    :param input: input file path.
    :param identifier: TableNumber that is added as column before writing the table.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables, if
     passed explicitly.
    :pandas_df: pandas dataframe generated from modified .csv file
    """
    # Uses original approach to generating pandas dataframe
    # with temporary directory using backports.tempfile.TemporaryDirectory()
    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()

    with backports.tempfile.TemporaryDirectory() as directory:
        tmp_source = os.path.join(directory, os.path.basename(input))
        with open(input, "r") as fin, open(tmp_source, "w") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            headers = next(reader)
            if not skip_table_prefix:
                headers = [__format__(name, header) for header in headers]
            # The first column is `TableNumber`, which is the unique identifier for the image CSV
            headers = ["TableNumber"] + headers
            writer.writerow(headers)
            [writer.writerow([identifier] + row) for row in reader]
            pandas_df = pd.read_csv(tmp_source)
    return pandas_df


def get_df(input, identifier, skip_table_prefix=False):
    """
    New version of get_df_from_temp_dir(): Reads .csv as pandas dataframe directly,
    modifies dataframe (by adding header prefix to existing columns and by
    adding a new column containing the TableNumber). Does not use a temporary
    directory. Returns modified dataframe.

    :param input: input file path.
    :param identifier: TableNumber that is added as column before writing the table.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables, if
     passed explicitly.
    :dataframe: pandas dataframe that is loaded, modified and returned.
    """
  # More explanations:
  # Returns the pandas data frame of the modified csv (prefixed column headers).
  # This function is an alternative to get_df_from_temp_dir().
  # - It does not use a temporary location backports.tempfile.TemporaryDirectory()
  # - it imports csv  -> Pandas dataframe directly
  # - it modifies the table just like get_df_from_temp_dir(), but in Pandas DF format. [tested!]
  # - it does not use the helper function __format__(name, header), which converts all types to "object"

    print("------------------- in get_df() ----------------")
    print("input: ", input)
    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()
    # read into DF
    dataframe = pd.read_csv(input)  # remove index_col=0 !!
    # add "name"prefix to column headers
    if not skip_table_prefix:
        no_prefix = ["ImageNumber", "ObjectNumber"]  # exception rows
        dataframe.columns = [
            i if i in no_prefix else name + "_" + i for i in dataframe.columns
        ]
    # add TableNumber
    number_of_rows, _ = dataframe.shape
    table_number_column = [identifier] * number_of_rows  # create additional column
    dataframe.insert(0, "TableNumber", table_number_column, allow_duplicates=False)
    return dataframe


def type_convert_dataframe(dataframe, config_file):
    """
    Type casting of entire pandas dataframe.
    Calls conversion function based on specifications in configuration file.
    :param dataframe: input file
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))
    """
    # simple, explicit type conversion
    type_conversion = config_file["schema"]["type_conversion"]
    if type_conversion == "int2float":
        dataframe = convert_cols_int2float(dataframe)
    elif type_conversion == "all2string":
        dataframe = convert_cols_2string(dataframe)
    return dataframe


def checksum(pathname, buffer_size=65536):
    """
    Generate a 32-bit unique identifier for a file.
    :param pathname: input file
    :param buffer_size: buffer size
    """
    with open(pathname, "rb") as stream:
        result = zlib.crc32(bytes(0))
        while True:
            buffer = stream.read(buffer_size)
            if not buffer:
                break
            result = zlib.crc32(buffer, result)
    return result & 0xFFFFFFFF


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
            ref_df = get_df(
                input=path,
                identifier=refIdentifier,
                skip_table_prefix=skip_image_prefix,
            )
        else:
            ref_df = get_df(input=path, identifier=refIdentifier)
        # ---------------------- temporary -------------------------------------
        refPyTable_before_conversion = pyarrow.Table.from_pandas(ref_df)
        ref_schema_before_conversion = refPyTable_before_conversion.schema[0]
        print("------ In open_writers(): ref_schema_before_conversion --------")
        # print(ref_schema_before_conversion)
        # ----------------------------------------------------------------------
        type_conversion = config_file["schema"]["type_conversion"]
        print(
            "------ In open_writers(): type_conversion: ", type_conversion, "--------"
        )
        if type_conversion == "int2float":
            ref_df = convert_cols_int2float(
                ref_df
            )  # converts all columns int -> float (except for "TableNumber")
            print("------ converted ref_pandas_df to float --------")
        elif type_conversion == "all2string":
            ref_df = convert_cols_2string(ref_df)
            print("------ converted ref_pandas_df to string --------")

        ref_table = pyarrow.Table.from_pandas(
            ref_df
        )
        ref_schema = ref_table.schema
        # print("------ In open_writers(): ref_schema (after conversion)")
        ref_schema_after_conversion = ref_schema[0]
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
    print(" ------------------- Leaving get_reference_paths() -------------------")
    return ref_dirs


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
            ####################################################################
            # Exception: Do not convert these columns from int to float
            keep_int = ["ImageNumber", "ObjectNumber", "TableNumber"]
            if name in keep_int:
                continue
            pandas_df[name] = pandas_df[name].astype("float")
    return pandas_df


def convert_cols_2string(dataframe):
    """
    Converts all column types to 'string'.
    :pandas_df: Pandas dataframe
    """
    # iterates over the columns of the input Pandas dataframe
    # and converts all values to type string
    for col_name in dataframe.columns:
        dataframe[col_name] = dataframe[col_name].astype("str")
    return pandas_df


def get_full_paths_in_dir(directory, config_file, name=None):
    # function wrapped around previous code
    """
    Calls csv-validation function and return full paths as two arguments,
     a list and a string, for compartments and image separately.
    :directory: path to directory containing .csv files, e.g. "path/plate_a/set_1/"
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))

    """
    # compartments: list of strings (full paths to compartment csv files in directory)
    # image:  string (full path to image.csv in directory)
    # We return two separate arguments.
    # NOTE:
    # - Changes s.t. it does not return None values
    # - Would prefer to replace the function by get_dict_of_paths(), which returns a dictionary.
    # cytominer_database.utils.validate_csv_set(config_file, directory) is a rabbit hole!

    try:
        compartments, image = cytominer_database.utils.validate_csv_set(
            config_file, directory
        )
    except IOError as e:
        click.echo(e)
        return
    # get rid of None values in compartments:
    compartments_without_None_values = [comp for comp in compartments if comp]
    # check image for being a None value:
    if image:
        return compartments_without_None_values, image
    else:
        return compartments_without_None_values




def seed(source, target, config_path, skip_image_prefix=True, directories=None):
    """
    Main function. Loads configuration. Opens ParquetWriter.
    Calls writer function. Closes ParquetWriter.
    :source: path to parent directory containing subdirectories, e.g. "path/plate_a/"
    :config_path: path to configuration file config.ini
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    :directories: Pass subdirectories path list instead of generating a
        path list from the source path. Added to test special cases. Can be removed.

    """
    # Note: prefixes are never skipped for compartments.
    # They are skipped for images by default, but can be added if skip_image_prefix is passed as True.
    # get backend option

    config = cytominer_database.utils.read_config(config_path)
    engine = config["database_engine"]["database"]
    print("------ in seed_modified -------- ")
    print("engine = ", engine)
    # oper the Parquet writer, using a schema that all tables will be aligned with
    if engine == "Parquet":
        # dictionary that contains [name]["writer"], [name]["schema"], [name]["pandas_dataframe"]
        print("------ in seed_modified: going to open writers")
        writers_dict = open_writers(source, target, config, skip_image_prefix)

    print("---------------- in seed(): opened all writers --------------------")
    # temporary for debugging purposes: option to pass directories as a list
    # ...instead of creating the list internally fron 'source'.
    if not directories:
        directories = sorted(
            list(cytominer_database.utils.find_directories(source))
        )  # lists the subdirectories that contain CSV files
    print("directories: ")
    print(directories)
    # ---------------------------------------------------------------------------
    for directory in directories:
        # ----------------------------- SQLite ---------------------------------
        if engine == "SQLite":
            # ....................... get input file paths ......................
            try:
                compartments, image = cytominer_database.utils.validate_csv_set(
                    config, directory
                )
            except IOError as e:
                click.echo(e)
                continue
            # ....... get identifier for entire directory (TableNumber) .........
            identifier = checksum(
                image
            )  # here we assume every directory holds a image.csv
            # ............................. image-csv ...........................
            try:
                write_csv_to_SQLite(
                    input=image,
                    output=target,
                    identifier=identifier,
                    skip_table_prefix=skip_image_prefix,
                )
            except sqlalchemy.exc.DatabaseError as e:
                click.echo(e)
                continue
            # ...........................compartments-csv .......................
            for compartment in compartments:
                write_csv_to_sqlite(
                    input=compartment, output=target, identifier=identifier
                )  # skip_table_prefix=False by default
        # ----------------------------- Parquet --------------------------------
        elif engine == "Parquet":
            # ....................... get input file paths ......................
            compartments, image = get_full_paths_in_dir(directory, config)
            # note: get_full_paths_in_dir() was modified to return only values that are not None
            # Hence, if image.csv is missing in the directory, we'll get an error at this assignment.

            # ....... get identifier for entire directory (TableNumber) .........
            identifier = checksum(
                image
            )  # here we assume every directory holds a image.csv
            # ............................. image-csv ...........................
            write_csv_to_parquet(
                input=image,
                output=target,
                identifier=identifier,
                writers_dict=writers_dict,
                skip_table_prefix=skip_image_prefix,
            )

            # .......................... compartments-csv .........................
            for compartment in compartments:  #
                write_csv_to_parquet(
                    input=compartment,
                    output=target,
                    identifier=identifier,
                    writers_dict=writers_dict,
                )
    # --------------------- close writers ---------------------------------------
    if config["database_engine"]["database"] == "Parquet":
        for name in writers_dict.keys():
            writers_dict[name]["writer"].close()
    print(
        "&&&&&&&&&&&&&&&&&&&&&&&&& closed writers &&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
    )
