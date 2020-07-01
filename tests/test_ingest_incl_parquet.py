import os

import pandas as pd
import backports.tempfile
from sqlalchemy import create_engine

# import cytominer_database.ingest
import cytominer_database.ingest_incl_parquet
import cytominer_database.munge
import pytest


@pytest.mark.parametrize("config_choice", ["config_Parquet.ini", "config_SQLite.ini"])
def test_seed(dataset, config_choice):
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]
    config_path = os.path.join(data_dir, config_choice)
    # Get database engine option
    # (for this we need to read the config file from the path here already!)
    config_file = cytominer_database.utils.read_config(config_path)
    engine_type = config_file["ingestion_engine"]["engine"]

    if munge:
        cytominer_database.munge.munge(config_path, data_dir)

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # set up
        if engine_type == "Parquet":
            # create output directory
            target = os.path.join(temp_dir, "test_parquet_output")
            print("Parquet target = ", target)
            try:
                os.stat(target)
            except:
                os.mkdir(target)

        elif engine_type == "SQLite":
            sqlite_file = os.path.join(temp_dir, "test.db")
            target = "sqlite:///{}".format(str(sqlite_file))
            print("SQL target = ", target)

        # run program
        cytominer_database.ingest_incl_parquet.seed(
            config_path=config_path, source=data_dir, output_path=target
        )
        # necessary ?
        if engine_type == "Parquet":
            assert os.path.exists(str(target))
        elif engine_type == "SQLite":
            assert os.path.exists(str(sqlite_file))

        for blob in ingest:
            table_name = blob["table"].capitalize()

            if engine_type == "Parquet":
                # df = pd.read_parquet(path=target, engine ='pyarrow') # ignore column 0 (columns=[1:])? Column 0 should be read only as an index (index_col=0) ?
                basename = ".".join([table_name, "parquet"])
                compartment_path = os.path.join(target, basename)
                df = pd.read_parquet(path=compartment_path)

            elif engine_type == "SQLite":
                engine = create_engine(target)
                con = engine.connect()
                df = pd.read_sql(sql=table_name, con=con)

            print(
                "In test_ingest_incl_parquet(). Loading from target = {}, reading table_name = {}".format(
                    target, table_name
                )
            )
            print(df[:])
            print("df.shape[0] , blob['nrows'] : ", df.shape[0], blob["nrows"])
            print("df.shape[1] , blob['ncols'] : ", df.shape[1], str(blob["ncols"] + 1))
            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert (
                    df.groupby(["TableNumber", "ImageNumber"]).size().sum()
                    == blob["nrows"]
                )
