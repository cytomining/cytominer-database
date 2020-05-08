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

    cytominer_database.ingest.seed(source, target, config_path)

    where
    source = path/to/plate_a
    target = path/to/output/directory
    config_path = path/to/plate_a/ingest_config.ini
Notes: 
* seed() will run regardless of where the config-file was stored, as long as its' full path is passed. 
This is not the case for the command-line command, where the config-file is specified only by the basename 
(e.g. ingest_config.ini) and is assumed to reside in path/to/plate_a.

* 'file_1.csv', ..., 'file_n.csv' is typically equivalent to 'Cells.csv', 'Cytoplasm.csv', 'Nuclei.csv', 'Image.csv', 'Object.csv'.

"""

"""
* Extended explanations of how the PARQUET writer is used (see also the readme.rst).
[Big Picure]
When opening a Parquet file with the Pyarrow.parquet.writer, we use a reference table to set
one fixed table schema (pyarrow.Table.schema) (per table kind).
[Schema compatibility: column names]
For each new table to be written to the open Parquet writer, schema compatibility is assured by aligning the dataframes. 
The alignment assures that the new table will hold all columns present in the reference table, however, columns not present in the 
reference table will be dropped from the new table. 
Hence it is important to assure that the reference table has all columns that will be present in any of the tables of its kind. 

[Schema compatibility: column types]
Furthermore, the column types of columns with identical column names must agree. This has proven to be problematic
when software further up the processing pipeline casts the types of its' output automatically. 
The easiest solution for this is to convert all values in the .csv table to strings, however, this can be a disadvantage in the 
processing tasks futher down the pipeline. The type incompatipility occurs infrequently though,
 e.g. when a 0-valued column is cast as 'int', instead of 'float', which makes the table schema incompatible with
  the table schema from files with non-zero values in the same column with a 'float'-type). 
 Hence it is in our experience sufficient to convert integers to float-types. 
 The type conversion is applied explicitly and directly to both the reference table before opening the writer,
  and to any table before appending it to the open writer. 
 The conversion type is set in the config file in the schema section, with key type_conversion.
  Possible values are 'int2float' and 'all2string'. We do not use any automatic type-hierarchy-based casting
 (although this code was developed in the preliminary colaboratory notebook). 
 Note that schema incompatibilities are currently only solved for the Parquet option,
  and are not resolved when writing the files to SQL with the SQLite option. 

[Choosing the reference tables]
The reference table is set by either
(a) The widest table among a subset of tables sampled at random 
(from all subdirectories of the source folder, e.g. "plate_a/" -> .csv files in set_1, ..., set_m ). 
The sample size is determined as a fraction of the number of all available files of that kind,
the value of which is a number in [0,1] and is set in the config file with the key "ref_fraction".
The tables are sampled iid within one table kind and independent of other table kinds. 
or
(b) The reference tables are stored in a prespecified folder located at "reference_option = path/to/reference/folder", 
as specified in the config file (path is relative to source_directory).

[Reference dictionary keys]
To avoid errors from inconsistent capitalization of .csv file names, the keys of the reference dictionaries
(referencing the writer and the fixed schema) are built automatically from the basenames of existing CSV files
in specified directories. This means that per default, object.csv is also read and written.
However, this can be surpressed by explicitly excluding them in write.csv_to_sqlite() and write.csv_to_parquet().
 
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
    :config_path: full path of configuration file, e.g. path/to/plate_a/ingest_config.ini
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
                cytominer_database.utils.type_convert_dataframe(dataframe, config, engine)
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
