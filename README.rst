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
