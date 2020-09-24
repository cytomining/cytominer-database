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

Example:

    cytominer_database.ingest.seed(source, target, config_path)

    where
    source = path/to/plate_a
    target = path/to/output/directory
    config_path = path/to/plate_a/ingest_config.ini
Example: 
 'file_1.csv', ..., 'file_n.csv' could be equivalent to 'Cells.csv', 'Cytoplasm.csv', 'Nuclei.csv', 'Image.csv', 'Object.csv'.

"""
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
import collections
import numpy as np
import cytominer_database
import cytominer_database.utils
import cytominer_database.write
import cytominer_database.tableSchema


def seed(
    source,
    output_path,
    config_path,
    skip_image_prefix=True,
    sqlite=False,
    parquet=False,
):
    """
    Main function. Loads configuration. Opens ParquetWriter.
    Calls writer function. Closes ParquetWriter.
    :source: path to parent directory containing subdirectories, e.g. "path/plate_a/"
    :output_path: path of destination folder
    :config_path: full path of configuration file, e.g. path/to/plate_a/ingest_config.ini
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    :sqlite: True if SQLite is used as backend engine
    :parquet: True if Parquet is used as backend engine

    """
    config = cytominer_database.utils.read_config(config_path)
    engine = get_engine(sqlite=sqlite, parquet=parquet)

    # get dictionary that contains [name]["writer"], [name]["schema"], [name]["pandas_dataframe"]
    writers_dict = cytominer_database.tableSchema.open_writers(
        source, output_path, config, engine, skip_image_prefix
    )
    # lists the subdirectories that contain CSV files
    directories = sorted(list(cytominer_database.utils.find_directories(source)))
    # ----------------------------- iterate over subfolders in source folder------------------------------------
    for directory in directories:
        # ....................... get input .csv file paths ......................
        try:
            compartments, image = cytominer_database.utils.validate_csv_set(
                config, directory
            )
        except IOError as e:
            click.echo(e)
            continue

        identifier = checksum(image)
        csv_locations = [image] + compartments
        # ----------------------------------- iterate over .csv's ---------------------------------------
        for input_path in csv_locations:
            table_name = cytominer_database.utils.get_name(input_path)
            dataframe = cytominer_database.load.get_and_modify_df(
                input_path, identifier, skip_image_prefix
            )
            cytominer_database.utils.type_convert_dataframe(
                dataframe=dataframe, engine=engine, config=config
            )  # only for Parquet
            cytominer_database.write.write_to_disk(
                dataframe, table_name, output_path, engine, writers_dict
            )
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


def get_engine(sqlite=False, parquet=False):
    """
    
    :param sqlite: True if sqlite is selected as backend
    :param parquet: True is parquet is selected as backend
    
    """
    if sqlite and parquet:
        raise ValueError(
            "Two command flags '--parquet' and '--sqlite' cannot be added simultaneously."
        )
    elif sqlite:
        engine = "SQLite"
    elif parquet:
        engine = "Parquet"
    else:  # default
        engine = "SQLite"

    return engine
