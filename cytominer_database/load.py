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


def get_and_modify_df(input, identifier, skip_image_prefix):
    """
    Loads .csv as Pandas dataframe and returns modified pandas dataframe.


    :param input: input file path. 
    :param identifier: TableNumber that is added as column before writing the table.
    :skip_image_prefix: Boolean value specifying if the column headers of
     the image.csv files should be prefixed with the table name ("Image").
    """
    # get dataframe
    dataframe = load_df(input)
    # get table name
    name = cytominer_database.utils.get_name(input)
    # add prefix to column names unless marked for skipping
    if (not skip_image_prefix) and name in ['Image', 'Object']:
        add_prefix(name, dataframe)
    # add identifier as an additional column called tableNumber
    add_tableNumber(dataframe, identifier)
    return dataframe


def load_df(input):
    """
    Reads .csv as Pandas dataframe directly.
    Does not use a temporary directory. Returns modified dataframe.

    :param input: input file path. 
    """
    # exit for files which are not valid (redundant check)
    if not cytominer_database.utils.validate_csv(input):
        warnings.warn("CSV could not be validated in get_df() for {}".format(input), UserWarning)
        return
    # read into DF
    dataframe = pd.read_csv(input)  # do not use index_col=0 !
    return dataframe

def add_prefix(name, dataframe):
    """
    Modifies dataframe by adding a header prefix to existing columns (adds "name" prefix to column headers)

    :param name: table name
    :param dataframe: dataframe to be modified
    """
    no_prefix = ["ImageNumber", "ObjectNumber", "TableNumber"]  # exception columns
    dataframe.columns = [
        i if i in no_prefix else f"{name}_{i}" for i in dataframe.columns
    ]
    return dataframe


def add_tableNumber(dataframe, identifier):
    """
    Modifies dataframe by adding a new column "TableNumber" containing the identifier. 
        
    :param dataframe: pandas dataframe that is loaded, modified and returned. 
    :param identifier: TableNumber that is added as column before writing the table.
    """
    number_of_rows, _ = dataframe.shape
    table_number_column = [identifier] * number_of_rows  # create additional column
    dataframe.insert(0, "TableNumber", table_number_column, allow_duplicates=False)

    # ---------------------- for debugging ---------------------- 
    # print("In get_df(): name = {} ; dataframe.shape = {}".format(name, dataframe.shape))
    # ------------------------------------------------------------
    return dataframe
