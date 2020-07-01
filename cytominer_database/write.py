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
import cytominer_database
import cytominer_database.utils
import cytominer_database.load


def write_to_disk(dataframe, table_name, output_path, engine, writers_dict):
    """
    Writes Pandas dataframes to SQLite or Parquet format.

    :param dataframe: pandas dataframe file in memory
    :param table_name: table name of dataframe/file
    :param output_path: location of SQLite/Parquet file
    :param engine: 'SQLite' or 'Parquet'
    :param writers_dict: dictionary storing references to the opened Parquet writer and schema
    """

    if engine == 'SQLite':
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)
            engine = create_engine(output_path)
            con = engine.connect()
            dataframe.to_sql(name=table_name, con=con, if_exists="append",  index = False)
    elif engine == 'Parquet':
        ref_dataframe = writers_dict[table_name]["pandas_dataframe"]
        dataframe, _ = dataframe.align(ref_dataframe, join="right", axis=1)
        # read into pyarrow table format
        table = pyarrow.Table.from_pandas(dataframe)
        # connect to writer and add current table
        writer = writers_dict[table_name]["writer"]
        writer.write_table(table)


""" --------- code snippets for testing code ---------
# -------- pandas dataframe alignment ----------------	
# (note: missing columns are added with same name and type
#  as in ref_dataframe, but containing NaN values.)	
dataframe, ref_dataframe_new = dataframe.align(ref_dataframe, join="right", axis=1)	
# assert that the reference table has not been modified by the alignment.
assert ref_dataframe_new.equals(ref_dataframe)
# --------- identical pandas schemata-----------------	
assert dataframe.dtypes.equals(ref_dataframe.dtypes)
# --------- identical pyarrow schemata----------------	
# (note: use "==" for pyarrrow schema comparisons, not "is")
table = pyarrow.Table.from_pandas(dataframe)
assert (table.schema.types == writers_dict[name]["schema"].types)	
assert (table.schema.names == writers_dict[name]["schema"].names)
"""
