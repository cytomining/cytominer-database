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

import os
import csv
import click
import warnings
import zlib

import pandas as pd
import backports.tempfile
import sqlalchemy.exc
from sqlalchemy import create_engine
import cytominer_database.utils

import pyarrow
import pyarrow.parquet as pq
import pyarrow.csv


def __format__(name, header):
    if header in ["ImageNumber", "ObjectNumber"]:
        return header

    return "{}_{}".format(name, header)


def into(input, output, name, identifier, skip_table_prefix=False, writers):
    """Ingest a CSV file into a table in a database.

    :param input: Input CSV file.
    :param output: Connection string for the SQL database or Parquet directory
    :param name: Table in database into which the CSV file will be ingested
    :param identifier: Unique identifier for ``input``.
    :param skip_table_prefix: True if the prefix of the table name should be excluded
     from the names of columns.
    """

    with backports.tempfile.TemporaryDirectory() as directory: 
        # Question: Why do we need to reload the directory and generate the source? 
        # Why is "source" used as "fout"-writer later ?
        # Isn't Source == input ? ("input" seems to have exactly the "directory + basename"-structure?)
        source = os.path.join(directory, os.path.basename(input))

        # Question: Can we move this section into the "SQLite" case?
            # create a temporary CSV file which is identical to the input CSV file
            # but with the column names prefixed with the name of the compartment
            # (or `Image`, if this is an image CSV file, and `skip_table_prefix` is False)
        with open(input, "r") as fin, open(source, "w") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            headers = next(reader)
            if not skip_table_prefix:
                headers = [__format__(name, header) for header in headers]

            # The first column is `TableNumber`, which is the unique identifier for the image CSV
            headers = ["TableNumber"] + headers
            writer.writerow(headers)
            [writer.writerow([identifier] + row) for row in reader]

        # The argument "writers" is an empty dict {} iff "SQLite" was selected as ingestion engine 
        if writers == {} :
            # Now ingest the temp CSV file (with the modified column names) into the database backend
            # the rows of the CSV file are inserted into a table with name `name`.
            with warnings.catch_warnings():
                # Suppress the following warning on Python 3:
                #
                #   /usr/local/lib/python3.6/site-packages/odo/utils.py:128: DeprecationWarning: inspect.getargspec() is
                #     deprecated, use inspect.signature() or inspect.getfullargspec()
                warnings.simplefilter("ignore", category=DeprecationWarning)
                engine = create_engine(output)
                con = engine.connect()
                df = pd.read_csv(source, index_col=0)
                df.to_sql(name=name, con=con, if_exists="append")
        else: # Write into open Parquet writers which are stored in the dictionary   
            table           = pyarrow.csv.read_csv(source)
            parquetWriter   = writers[name]  # will return the opened writer of the image or compartment
            parquetWriter.write_table(table)

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

    return result & 0xffffffff

def getWriters(source, target, tablePaths, writer_dict):
    # Comment:  Here we use a dictionary for four different writers,
    #           ..where key = compartment and  value = opened writer.
    #           Alternatively, we can dynamically allocate the writer
    #           ..variable with:  globals()["writer_" + name ] = ...

    for tablePath in tablePaths:  # tablePaths = [image, compartments] 
        name, _             = os.path.splitext(os.path.basename(tablePath)) # why do we need splitext?

        if name in writer_dict :    # close writer
            writer = writer_dict[name]
            writer.close()
        else:                       # open writer
            table               = pyarrow.csv.read_csv(tablePath)
            destination         = os.path.join(target,name,".parquet") # ToDo: make sure 'target' ends with '/'
            writer_dict[name]   = pq.ParquetWriter(destination, table.schema)

    return writer_dict


def seed(source, target, config_file, skip_image_prefix=True):
    """
    Read CSV files into a database backend.

    :param config_file: Configuration file.
    :param source: Directory containing subdirectories that contain CSV files.
    :param target: Connection string for the SQLite database
                   OR directory string for parquet output
    :param skip_image_prefix: True if the prefix of image table name should be excluded
     from the names of columns from per image table
    """
    # moved this to top level:
    # config_file = cytominer_database.utils.read_config(config_file)
    # list the subdirectories that contain CSV files
    directories = sorted(list(cytominer_database.utils.find_directories(source)))
    # get ingestion engine type 
    engine  = os.path.splitext(config_file["database_engine"]["database"])  

    for i, directory in enumerate(directories):
        # get the image CSV and the CSVs for each of the compartments
        try:
            compartments, image = cytominer_database.utils.validate_csv_set(config_file, directory)
        except IOError as e:
            click.echo(e)

            continue
            # get a unique identifier for the image CSV. This will later be used as the TableNumber column
            # the casting to int is to allow the database to be readable by CellProfiler Analyst, which
            # requires TableNumber to be an integer.
        identifier = checksum(image)

        # If first file: Initialize dictionary containing writers                
        if i == 0 :
            writer_dict = {}
            if engine == 'Parquet' :
                writer_dict = getWriters(target, tablePaths=[image, compartments]) 

        # start ingestion
        # 1. ingest the image CSV
        name, _ = os.path.splitext(config_file["filenames"]["image"])
            # Claim: 
            # os.path.splitext(config_file["filenames"]["image"]) == os.path.splitext(os.path.basename(image))
            
            # Proposal:
            # 1. Since we use "name, _ = os.path.splitext(os.path.basename(compartment)) " 
            # .. for the compartments, let's use  "name, _ = os.path.splitext(os.path.basename(image))" 
            # .. for the image-part too ?
            # 2. Unite the ingestion of image and compartment csv's ? 
            # 3. Find a way to work around the skip_table_prefix, 
            # .. which is passed by different default values for seed() and into()
        """
        for table in [image, compartments]:
            name, _ = os.path.splitext(os.path.basename(table))
            if name is not "Image":
                skip_image_prefix = False 
            try:
                into(input=table, output=target, name=name.capitalize(), identifier=identifier,
                        skip_table_prefix=skip_image_prefix, writers=writer_dict)
            except sqlalchemy.exc.DatabaseError as e:
                click.echo(e)
                continue
        """
        try:
            into(input=image, output=target, name=name.capitalize(), identifier=identifier,
                    skip_table_prefix=skip_image_prefix, writers=writer_dict)
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)
            continue

        # 2. ingest the CSV for each compartment
        for compartment in compartments:
            name, _ = os.path.splitext(os.path.basename(compartment))
            into(input=compartment, output=target, name=name.capitalize(), identifier=identifier,
                    writers=writer_dict)

        #close Parquet writer after last file has been ingested
        if i == (len(directories)-1) and engine == 'Parquet':
            writer_dict = getWriters(target, tablePaths=[image, compartments], writer_dict)




