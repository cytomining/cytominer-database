This package is deprecated and will no longer be supported. Please use at your own risk!

==================
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
=======================
While tools like CellProfiler can store measurements directly in databases, it is usually infeasible to create a
centralized database in which to store these measurements. A more scalable approach is to create a set of CSVs per
“batch” of images, and then later merge these CSVs.

cytominer-database ingest reads these CSVs, checks for errors, then ingests
them into a database backend. The default backend is `SQLite`.

.. code-block:: sh

	cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini

will ingest the CSV files nested under source_directory into a `SQLite` backend
To select the `Parquet` backend add a `--parquet` flag:

.. code-block:: sh

	cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini --parquet

The ingest_config.ini file then needs to be adjusted to contain the `Parquet` specifications.

How to use the configuration file
=================================
The configuration file ingest_config.ini must be located in the source_directory and can be modified to specify the ingestion.
There are three different sections.

The [filenames] section
-----------------------

.. code-block::

  [filenames]
  image   = image.csv      #or: Image.csv
  object  = object.csv     #or: Object.csv

cytominer-database is currently limited to the following measurement file kinds:
Cells.csv, Cytoplasm.csv, Nuclei.csv, Image.csv, Object.csv.
The [filenames] section in the configuration file saves the correct basename of existing measurement files.
This may be important in the case of inconsistent capitalization.

The [database_engine] section
-----------------------------

.. code-block::

  [ingestion_engine]
  engine = Parquet      #or: SQLite

The [database_engine] section specifies the backend.
Possible key-value pairs are:
**engine** = *SQLite* or **engine** = *Parquet*.

The [schema] section
--------------------

.. code-block::

 [schema]
 reference_option = sample         #or: path/to/reference/folder relative to source_directory
 ref_fraction     = 1              #or: any decimal value in [0, 1]
 type_conversion  = int2float      #or: all2string

The [schema] section specifies how to manage incompatibilities in the table schema of the files.
In that case, a Parquet file is fixed to a schema with which it was first opened, i.e. by the first file which is written (the reference file).
To append the data of all .csv files of that file-kind, it is important to assure the reference file satisfies certain incompatibility requirements.
For example, make sure the reference file does not miss any columns and all existing files can be automatically converted to the reference schema.
Note: This section is used only if the files are ingested to Parquet format and was developed to handle the special cases in which tables that cannot be concatenated automatically.

There are two options for the key **reference_option**:

The first option is to create a designated folder containing one .csv reference file for every kind of file ("Cytoplasm.csv", "Nuclei.csv", ...) and save the folder path in the config file as **reference_option** = *path/to/reference/folder*, where the path is relative to the source_directory from the ingest command.
These reference files' schema will determine the schema of the Parquet file into which all .csv files of its kind will be ingested.


**This option relies on manual selection, hence the chosen reference files must be checked explicitly: Make sure the .csv files are complete in the number of columns and contain no NaN values.**

Alternatively, the reference files can be found automatically from a sampled subset of all existing files.
This is the case if **reference_option** = *sample* is set.
A subset of all files is sampled uniformly at random and the first table with the maximum number of columns among all sampled .csv files is chosen as the reference table.
If this case, an additional key **ref_fraction** can be set, which specifies the fraction of files sampled among all files.
The default value is **ref_fraction** = *1* , for which all tables are compared in width.
This key is only used if "reference_option=sample".

Lastly, the key **type_conversion** determines how the schema types are handled in the case of disagreement.
The default value is *int2float*, for which all integer columns are converted to floats.
This has been proven helpful for trivial columns (0-valued column), which may be of "int" type and cannot be written into the same table as non-trivial files with non-zero float values.
Automatic type conversion can be avoided by converting all values to string-type.
This can be done by setting **type_conversion** = *all2string*.
However, the loss of type information might be a disadvantage in downstream tasks.
