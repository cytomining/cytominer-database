cytominer-database
==================

.. image:: https://travis-ci.org/cytomining/cytominer-database.svg?branch=master
    :target: https://travis-ci.org/cytomining/cytominer-database
    :alt: Build Status

.. image:: https://readthedocs.org/projects/cytominer-database/badge/?version=latest
    :target: http://cytominer-database.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

cytominer-database provides command-line tools for organizing measurements extracted from images.

Software tools such as CellProfiler can extract hundreds of measurements from millions of cells in a typical
high-throughput imaging experiment. The measurements are stored across thousands of CSV files.

cytominer-database helps you organize these data into a single database backend, such as SQLite.

Why cytominer-database?
-----------------------
While tools like CellProfiler can store measurements directly in databases, it is usually infeasible to create a
centralized database in which to store these measurements. A more scalable approach is to create a set of CSVs per
“batch” of images, and then later merge these CSVs.

cytominer-database ingest reads these CSVs, checks for errors, then ingests them into a database backend, including
SQLite, MySQL, PostgresSQL, and several other backends supported by odo.

.. code-block:: sh

	cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini

will ingest the CSV files nested under source_directory into a SQLite backend

How to use the configuration file
---------------------------------
The configuration file ingest_config.ini must be located in the source_directory and can be modified to specify the ingestion.
There are three different sections, the first being:

.. code-block:: 

  [filenames]
  image = image.csv
  object = object.csv

Cytominer-Database is currently limited to the following measurement file kinds: Cells.csv, Cytoplasm.csv, Nuclei.csv, Image.csv, Object.csv.
The [filenames] section in the configuration file saves the correct basename of existing measurement files
(this may be important in the case of inconsistent capitalization). # TODO: Are there any other reasons for the [filenames] section?

.. code-block::

 [schema]
 reference_option = sample
 ref_fraction = 1
 type_conversion = int2float

The [schema] section specifies how to manage incompatibilities in the file schemas.
The key "reference_option" can be set to the path of a folder which contains exactly
one .csv file for every kind of measurement file to be ingested.
These files will then be used as reference files to build a schema which fits
all existing files (i.e. to which all files can be converted automatically).
Hence it is important that the files have the correct column names and types and do not miss any columns.
Alternatively, the reference files can be found automatically from a sampled subset of all existing files.
For this a subset of all files is sampled uniformly at random, among which
the file with the maximum number of columns is chosen as a reference file.
For this, the key "reference_option" takes the value "sample" and in addition the key
"ref_fraction" can be set to any real number in [0,1], specifying the fraction of files
sampled among all files. The default value is '1' (all tables are compared in width).
The key "type_conversion" determines how the schema types are handled in the case of disagreement.
The default value is "type_conversion=int2float", in for which all integer columns are converted to floats.
This has been proven helpful for trivial columns (0-valued column), which may be of "int" type
and cannot be written into the same table as non-trivial files with non-zero float values.
A good way to avoid automatic type conversion is to convert all values to string-type.
This can be done by setting "type_conversion=all2string".
However, the loss of type information might be a disadvantage in downstream tasks.


.. code-block::

  [database_engine]
  database = Parquet

The [database_engine] section specifies the backend. Possible key-value pairs are:
"database=SQLite" or "database=Parquet".[Maybe ToDo: Replace with command flag]
