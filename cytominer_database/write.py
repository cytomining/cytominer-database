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
import  cytominer_database.load


def csv_to_sqlite(input, output, identifier,  skip_table_prefix=False):
    """
    Gets a modified dataframe, opens an engine and writes to SQLite

    :param input: input file path
    :param output: output file path
    :param identifier: TableNumber that is added as column. Argument is only passed down.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables,
     if passed explicitly.
    """
    # Note on skip_table_prefix:
    # Per default, header prefixes are not added to image.csv tables.
    # Header prefixes are always added to all other table types.
    # This is why the writing functions "write_csv_to_SQLite/Parquet()" call the
    # "image" separately from "compartments"
    # (in the first case, the argument skip_image_prefix is passed).
    # An alternative would be to remove this case distinction and not pass skip_image
    # as an argument. The option can be read from the config file directly before
    # modifying the headers. This would require passing the config file along,
    # which is already done in many functions, since the ParquetWriter introduces
    # many different options in config.ini.
    # Also note that we use skip_table_prefix and skip_image_prefix interchangebly.

    # -------------- check if filepath leads to valid file --------------------
    if not cytominer_database.utils.validate_csv(input): return
    # -------------------------------------------------------------------------

    name, _ = os.path.splitext(os.path.basename(input))
    name    = name.capitalize()
    # --------special case---------
    # if name == "Object": return
    # ----------------------------

    print("............... in write_csv_to_sqlite:", name, "..................")
    # get dataframe
    dataframe = cytominer_database.load.get_df_from_temp_dir(input, identifier, skip_table_prefix)
    # Note: we could generate the identifier within get_df_from_temp_dir(),
    # instead of passing it to write_csv_to_sqlite() and get_df_from_temp_dir().
    # However, we need to access the image.csv in the parent directory,
    # which can get messy if we're writing another table kind
    # (e.g.input = ....../Cells.csv ----> access  ....../image.csv)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        engine = create_engine(output)
        con = engine.connect()
        dataframe.to_sql(name=name, con=con, if_exists="append")

def csv_to_parquet(
    input, output, identifier, writers_dict, config_file , skip_table_prefix=False
):
    """
    Gets a modified dataframe and writes Table to opened ParquetWriter
    :param input: input file path
    :param output: output file path
    :param writers_dict : dictionary of dictionaries for opened ParquetWriters.
    Key: Name ; Values: {"writer" : ....}, {"schema": ...}, {"pandas_dataframe": ...}.
    :param identifier: TableNumber that is added as column before writing the table.
      Note: The paramenter is only passed down.
    :param skip_table_prefix: Per default the table name is added as a prefix
    to every column header. Skipping this can be true for image.csv tables, if
     passed explicitly. Note: The parameter is only passed down.
    """
    # -------------- check if filepath leads to valid file --------------------
    if not cytominer_database.utils.validate_csv(input): return
    # -------------------------------------------------------------------------

    name, _ = os.path.splitext(os.path.basename(input))
    name = name.capitalize()

    # --------special case---------
    # if name == "Object": return
    # ----------------------------


    print("............... in write_csv_to_parquet: ", name, ".................")
    print(input)
    # get dataframe
    dataframe = cytominer_database.load.get_df(input, identifier, skip_table_prefix)
    # --------debugging---------
    # print(dataframe.columns.get_loc("TableNumber")) # should be '0'
    # ----------------------------
    dataframe = cytominer_database.utils.type_convert_dataframe(dataframe, config_file)
    ref_dataframe = writers_dict[name]["pandas_dataframe"]
    # ---------alignment---------
    # Note: missing columns are added with same name and type as in ref_dataframe, but containing NaN values.
    dataframe, ref_dataframe_new = dataframe.align(ref_dataframe, join="right", axis=1)
    assert ref_dataframe_new.equals(
        ref_dataframe
    )  # here we assert that the reference table has not been modified by the alignment.
    # Alternatively, we can just do "dataframe,_ = dataframe.align(ref_dataframe, join="right", axis=1)

    # --------- temporary: check agreement of pandas df ------
    # check if the pyarrow schemata will be identical
    assert dataframe.dtypes.equals(ref_dataframe.dtypes)
    # ---------------------------------------------------------
    # read into pyarrow table format
    table = pyarrow.Table.from_pandas(dataframe)

    # --------- temporary: check agreement of pyarrow format ------
    # note: use "==" for schema comparisons, otherwise comparison may be False
    assert (table.schema.types == writers_dict[name]["schema"].types)
    assert (table.schema.names == writers_dict[name]["schema"].names)
    """
    if not (table.schema is writers_dict[name]["schema"]):
        print("---------------- == --------------")
        print("table.schema.names == writers_dict[name]['schema'].names")
        print(table.schema.names == writers_dict[name]['schema'].names)
        print("table.schema.types == writers_dict[name]['schema'].types")
        print(table.schema.types == writers_dict[name]['schema'].types )
        print("---------------- is --------------")
        print("table.schema.names is writers_dict[name]['schema'].names")
        print(table.schema.names is writers_dict[name]['schema'].names)
        print("table.schema.types is writers_dict[name]['schema'].types")
        print(table.schema.types is writers_dict[name]['schema'].types )
    """
    # ---------------------------------------------------------
    # get opened parquet writer, write table to file
    print(
        "------------",
        name,
        ": in write_csv_to_parquet: writing next table to Parquet file ------------",
    )
    writer = writers_dict[name]["writer"]
    writer.write_table(table)
    print(
        "------------",
        name,
        ": in write_csv_to_parquet: finished writing next table to Parquet file-------------",
    )
