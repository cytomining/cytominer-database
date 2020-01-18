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


# ToDo:
"""
- Build new schema
Regarding the names:
Load a random collection of tables and compare the number of columns.
Choose a table that belongs to the group with no missing columns. \
Leave an option to explicitly feed an example table.

Regarding the types:
Convert all column types int -> float (will be a small fraction of columns!)
(Potentially include later: Automatic comparison-based type casting)

- build example schema and pass it along. (Will get an error for conversions with loss!)

- Get pyarrow table enforcing a specific pyarrow schema
# use new_table = self.pa.Table.from_pandas(df, schema=table.schema) # check how this handles tables with missing columns or null values!

----> Then we don't need to explicitly concert the rows (of the pandas df)
 
 - build new pyarrow schema (test_modifying_schemata.ipynb ):
# get field
old_field = f1.schema[faulty_indices[0]]
# unpack
old_name, old_type = old_field.name, old_field.type
# pack
check_old_field = pa.field(old_name, old_type)
# check equivalence
old_field.equals(check_old_field) # True !!
#pyarrow schemata
assert(isinstance(f1.schema[index].type, int))
however
print(f1.schema[0].type) # int64 or double
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


def getDFfromTempDir(input, name, identifier, skip_table_prefix=False):
    with backports.tempfile.TemporaryDirectory() as directory:
        tmp_source = os.path.join(directory, os.path.basename(input))
            # create a temporary CSV file which is identical to the input CSV file
            # but with the column names prefixed with the name of the compartment
            # (or `Image`, if this is an image CSV file, and `skip_table_prefix` is False)
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
            pandasDataFrame = pd.read_csv(source, index_col=0) 
            return pandasDataFrame  

def dataFrame = convertCols_Int2Float(dataFrame):



def writeCSVtoDB(input, output, name, identifier,  writers, skip_table_prefix=False):
    """Ingest a CSV file into a table in a database.

    :param input: Input CSV file.
    :param output: Connection string for the SQL database or Parquet directory
    :param name: Table in database into which the CSV file will be ingested
    :param identifier: Unique identifier for ``input``.
    :param skip_table_prefix: True if the prefix of the table name should be excluded
     from the names of columns.
    """
    # Create modified .csv in a temporary directory and get Pandas DataFrame
    dataFrame = getModifiedDataframefromTempDir(input, name, identifier, skip_table_prefix=False)
    # Convert columns with integer values to float64 
    dataFrame = convertColsInt2Float(dataFrame)

    # If SQLite is the format specified in the config file, then the argument "writers" is an empty dict {}. 
            # Then ingest the temp CSV file (with the modified column names) into the database backend
            # the rows of the CSV file are inserted into a table with name `name`.
    if writers == {} :
        with warnings.catch_warnings():
        # Suppress the following warning on Python 3:
        #
        #   /usr/local/lib/python3.6/site-packages/odo/utils.py:128: DeprecationWarning: inspect.getargspec() is
        #     deprecated, use inspect.signature() or inspect.getfullargspec()
            warnings.simplefilter("ignore", category=DeprecationWarning)
            engine = create_engine(output)
            con = engine.connect()
            df.to_sql(name=name, con=con, if_exists="append")
    else: # Write into open Parquet writers which are stored in the dictionary
      


        # dataframe schema


        # compare to reference schema

        # type conversion if necessary

        # add empty columns if necessary






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

def openWriters(source, target,  writer_dict, identifier, refTablePaths):
    # Comment:  Here we use a dictionary for four different writers,
    #           ..where key = compartment and  value = opened writer.
    #           Alternatively, we can dynamically allocate the writer
    #           ..variable with:  globals()["writer_" + name ] = ...

    for refTablePath in refTablePaths:  # tablePaths = [image, compartments]
        name, _   = os.path.splitext(os.path.basename(refTablePath)) # why do we need splitext?
        name      = name.capitalize()
"""
At this point it would be easier to just get and pass the pyarrow schema. 
However, later we load the .csv files to pandas data frames and want to be able
to compare the "schemata" based on pandas data frames: If they disagree, we type-cast 
the relevant columns. Only then do we convert the data frames to pyarrow. 
Hence it is important to build a pandas dataframe for reference.
"""
    """

    - We need to find the *right* schema before opening the writer!
    - for now: 4 example files that have been converted and checked for completeness
    ;;; and use them to generate reference schema.
    - we need to store the pandas df schemata for reference later AND open a writer with a pyarrow schema
    - later: write code to find example files

    - Alternative : Write parquet files on an individual basis and deal with
    ... the schema mess later (merging parquet schemata)



    """ 
    # Get reference schema. 
    # During the loading and writing process, every table that has a different schema will be converted to the reference schema.
    # (1) Padding: Missing columns will be added with null-values
    # (2) Type casting: If a column type differs the column will be type-converted to the reference type.
    # Hence it is important that the reference tables from "refTablePaths" contain all columns and the columns are of the right type.
    # In our case this only means that integer types (values '0') are converted to float. 
    # I.e. we could just check all columns for ints and convert them to float.
            refPdDF                 = getDFfromTempDir(input=source, name=name, identifier, skip_table_prefix)
            refPdSchema             = 

            refPyTable              = pdDF.fromPandas()  #check exact function!
            basename                = name + ".parquet"
            # here: unpartitioned output directore
            destination         = os.path.join(target, basename)
            writer              = pq.ParquetWriter(destination, pyTable.schema)

            writer_dict[name]   = [writer, refPdSchema, refPySchema]

    return writer_dict

def closeWriters(source, target,  writer_dict, identifier, tablePaths):
    for tablePath in tablePaths:  # tablePaths = [image, compartments]
        name, _   = os.path.splitext(os.path.basename(tablePath)) # why do we need splitext?
        name      = name.capitalize()
        writer = writer_dict[name]
        writer.close()

def getrefSchema(directories, numberOfComparisons):
    # Takes the first n = numberOfComparisons+1 tables and finds the "common" schema. 




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
    engine  = config_file["database_engine"]["database"]

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
                tablePaths  = [image] + compartments
                writer_dict = openWriters(source, target, writer_dict, tablePaths)

        # start ingestion
        # 1. ingest the image CSV
        name, _ = os.path.splitext(config_file["filenames"]["image"])
        name      = name.capitalize()
        # one step :  name = os.path.splitext(config_file["filenames"]["image"])[0].capitalize
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
            into(input=image, output=target, name=name.capitalize(), identifier=identifier, writers=writer_dict,
                    skip_table_prefix=skip_image_prefix)
        except sqlalchemy.exc.DatabaseError as e:
            click.echo(e)
            continue

        # 2. ingest the CSV for each compartment
        for compartment in compartments:
            name, _ = os.path.splitext(os.path.basename(compartment))
            name      = name.capitalize()
            into(input=compartment, output=target, name=name.capitalize(), identifier=identifier,
                    writers=writer_dict)

        #close Parquet writer after last file has been ingested
        if i == (len(directories)-1) and engine == 'Parquet':
            tablePaths  = [image] + compartments
            writer_dict = getWriters(source, target, writer_dict, tablePaths)
