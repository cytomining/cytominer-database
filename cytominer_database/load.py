import os
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

#------ temporary solution to path import issues --------
#
# Loads individual tables into memory as pandas dataframe.
#
# -------------------------------------------------------




def __format__(name, header):
    """
    Adds a prefix to a column header. Returns modified header.
    :param name: string of table name
    :param header: column header
    """
    # exclude specific column headers
    if header in ["TableNumber", "ImageNumber", "ObjectNumber"]:
        return header
    return "{}_{}".format(name, header)

def get_df(input, identifier, skip_table_prefix=False):
    """
    New version of get_df_from_temp_dir(): Reads .csv as pandas dataframe directly,
    modifies dataframe (by adding header prefix to existing columns and by
    adding a new column containing the TableNumber). Does not use a temporary
    directory. Returns modified dataframe.

    :param input: input file path.
    :param identifier: TableNumber that is added as column before writing the table.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables, if
     passed explicitly.
    :dataframe: pandas dataframe that is loaded, modified and returned.
    """
  # More explanations:
  # Returns the pandas data frame of the modified csv (prefixed column headers).
  # This function is an alternative to get_df_from_temp_dir().
  # - It does not use a temporary location backports.tempfile.TemporaryDirectory()
  # - it imports csv  -> Pandas dataframe directly
  # - it modifies the table just like get_df_from_temp_dir(), but in Pandas DF format. [tested!]
  # - it does not use the helper function __format__(name, header), which converts all types to "object"

    # exit for files which are not valid (redundant check)
    if not cytominer_database.utils.validate_csv(input):
        warnings.warn("CSV could not be validated in get_df() for {}".format(input), UserWarning)
        return

    print("------------------- in get_df() ----------------")
    print("input: ", input)
    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()
    # read into DF
    dataframe = pd.read_csv(input)  # remove index_col=0 !!
    # add "name"prefix to column headers
    if not skip_table_prefix:
        no_prefix = ["ImageNumber", "ObjectNumber"]  # exception columns
        dataframe.columns = [
            i if i in no_prefix else name + "_" + i for i in dataframe.columns
        ]
    # add TableNumber
    number_of_rows, _ = dataframe.shape
    table_number_column = [identifier] * number_of_rows  # create additional column
    dataframe.insert(0, "TableNumber", table_number_column, allow_duplicates=False)
    print("In get_df(): name = {} ; dataframe.shape = {}".format(name, dataframe.shape))

    return dataframe

