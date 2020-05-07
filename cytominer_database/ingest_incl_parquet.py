# May 4th, TODO:
# Main: Clean up (remaiming: tableSchema.py). Build base class and engine-extensions.
# Other: - get rid of superfluous arguments. 
# ---> skip_table/image_prefix
    # alternative : instead of passing "skip_image_prefix", we could load the setting from the config file. Needs adjustments of tests.
    # Currently: prefixes are never skipped for compartments.
    # They are skipped for images by default, but can be added if skip_image_prefix is passed as True.
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
* General Notes:

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
- build tests that compare the table content, not just size
- build tests for these special cases
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

    # get dictionary that contains [name]["writer"], [name]["schema"], [name]["pandas_dataframe"]
    writers_dict = cytominer_database.tableSchema.open_writers(source, output_path, config, skip_image_prefix)
    # lists the subdirectories that contain CSV files
    if not directories:
        directories = sorted(
            list(cytominer_database.utils.find_directories(source))
        ) 
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
        # ----------------------------------- iterate over .csv's ---------------------------------------
        for input_path in csv_locations:
                table_name = cytominer_database.utils.get_name(input_path)
                dataframe = cytominer_database.load.get_and_modify_df(input_path, identifier, skip_image_prefix)
                dataframe = cytominer_database.utils.type_convert_dataframe(dataframe, config, engine)
                cytominer_database.write.write_to_disk(dataframe, table_name, output_path, engine, writers_dict)
    # --------------------------------------- close writers ---------------------------------------------
    close_writers(writers_dict, engine)
# --------------------------------------------- end ---------------------------------------------------


def close_writers(writers_dict, engine):
    """
    Close the Parquet writers
    :param writers_dict: dictionary containing the references to the writers of every table kind
    :param engine: "Parquet" or "SQLite
    """
    if engine == "Parquet":
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
