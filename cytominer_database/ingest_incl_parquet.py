# May 4th, TODO:
# Main: Clean up. Build base class and engine-extensions.
# Other: - get rid of superfluous arguments. 
#  ----> identifier
    # Update after restructuring : leave it this way
    # "Note: we could generate the identifier from the input file directly when it is needed. 
    # However, this requires accessing the image file of the same directory, so it might be 
    # easier to just read it out once and then pass it to the function.
    # Benefit: N
    # (e.g.input = ....../Cells.csv ----> access  ....../image.csv)""
# ---> skip_table/image_prefix
# an option would be : instead of passing "skip_image_prefix", we could load the setting from the config file. Needs adjustments of tests.
    # "Note on skip_table_prefix:
    # Per default, header prefixes are not added to image.csv and object.csv tables.
    # Header prefixes are always added to all other table types.
    # This is why the writing functions "write_csv_to_SQLite/Parquet()" call the
    # "image" separately from "compartments"
    # (in the first case, the argument skip_image_prefix is passed).
    # An alternative would be to remove this case distinction and not pass skip_image
    # as an argument. The option can be read from the config file directly before
    # modifying the headers. This would require passing the config file along,
    # which is already done in many functions, since the ParquetWriter introduces
    # many different options in config.ini.
    # Also note that we use skip_table_prefix and skip_image_prefix interchangebly. "
    #"    # Note: prefixes are never skipped for compartments.
    # They are skipped for images by default, but can be added if skip_image_prefix is passed as True.
    # get backend option"
# ---> how to deal with the config file 
        # "Notes:
        # (1) Input agument "config_path" is unparsed (just a path string).
        # We could also just load the config file once and pass it along,
        # instead of passing the string and reloading the file several times
        # i.e. call cytominer_database.utils.read_config(config_path) only once.
        # (2) The function argument name in the definition was changed as follows:
        # cytominer_database.utils.read_config(config_file) ---> cytominer_database.utils.read_config(config_path)
        # because config_file in read_config(config_file) is not a file, but a path to a file.
        # The function documentation has been changed accordingly."   
# ---> table name, input
        # Currently we are always calling "name = cytominer_database.utils.get_name(input)" 
        # Hence we're passing input as function arguments many often. Better: Attribute of class instance?


# ----> write a new validate_csv_set() and deal with weird image/compartment output structure         
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
- [DONE] parameter options read from config.ini --> see .txt file


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
- names and paths of CSV Files are stored in and passed as dictionaries, where the key is the
    capitalized table name (e.g. key = "Image" to store value ="path/set_1/image.csv").
    To account for cases where the capitalization is inconsistent or where some CSV files are missing,
    the dictionaries are built automatically from the basenames of existing CSV files in specified directories.
    This means that per default, object.csv is also read and written. However, this can be surpressed by explicitly
    excluding them in write.csv_to_sqlite() and write.csv_to_parquet().


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
import cytominer_database.write
import cytominer_database.tableSchema

#------ temporary solution to path import issues --------
#import sys
#sys.path.append("/Users/frances/git/cytominer-database")
# -------------------------------------------------------

def seed(source, output_path, config_path, skip_image_prefix=True, directories=None):
    """
    Main function. Loads configuration. Opens ParquetWriter.
    Calls writer function. Closes ParquetWriter.
    :source: path to parent directory containing subdirectories, e.g. "path/plate_a/"
    :output_path: path of destination folder
    :config_path: path to configuration file config.ini
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    :directories: Pass subdirectories path list instead of generating a
        path list from the source path. Added to test special cases. Can be removed.

    """
    config = cytominer_database.utils.read_config(config_path)
    engine = config["ingestion_engine"]["engine"]
    writers_dict = None # init
    if engine == "Parquet":
        # dictionary that contains [name]["writer"], [name]["schema"], [name]["pandas_dataframe"]
        writers_dict = cytominer_database.tableSchema.open_writers(source, output_path, config, skip_image_prefix)
    print("---------------- in seed(): opened all writers --------------------")
    if not directories:
        directories = sorted(
            list(cytominer_database.utils.find_directories(source))
        )  # lists the subdirectories that contain CSV files
    # ----------------------------- iterate over subfolders in source folder------------------------------------
    for directory in directories:
        # ....................... get input .csv file paths ......................
        try:
            compartments, image = cytominer_database.utils.validate_csv_set(config, directory)
        except IOError as e:
            click.echo(e)
            continue

        identifier = checksum(image)
        csv_locations = [image] + compartments
        # ----------------------------- iterate over .csv's ---------------------------------
        for input_path in csv_locations:
                table_name = cytominer_database.utils.get_name(input_path)
                dataframe = cytominer_database.load.get_and_modify_df(input_path, identifier, skip_image_prefix)
                dataframe = cytominer_database.utils.type_convert_dataframe(dataframe, config, engine)
                cytominer_database.write.write_to_disk(dataframe, table_name, output_path, engine, writers_dict)
    # --------------------- close writers ---------------------------------------
    if config["ingestion_engine"]["engine"] == "Parquet":
        for name in writers_dict.keys():
            writers_dict[name]["writer"].close()

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
