import os

import pandas as pd
import backports.tempfile
from sqlalchemy import create_engine

import cytominer_database.ingest_variable_engine
import cytominer_database.munge
import pytest


def test_seed_parquet(dataset):
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]    
    config_path = os.path.join(data_dir, "config.ini")


    if munge:
        cytominer_database.munge.munge(config_path, data_dir)

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # create output directory
        target = os.path.join(temp_dir, "test_parquet_output")
        try:
            os.stat(target)
        except:
            os.mkdir(target)
        # run program
        cytominer_database.ingest_variable_engine.seed(
            config_path=config_path, source=data_dir, output_path=target, parquet=True
        )

        assert os.path.exists(str(target))
        for blob in ingest:
            table_name = blob["table"].capitalize()
            # df = pd.read_parquet(path=target, engine ='pyarrow') # ignore column 0 (columns=[1:])? Column 0 should be read only as an index (index_col=0) ?
            basename = ".".join([table_name, "parquet"])
            compartment_path = os.path.join(target, basename)
            df = pd.read_parquet(path=compartment_path)
            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1
            if table_name.lower() != "image":
                assert (
                    df.groupby(["TableNumber", "ImageNumber"]).size().sum()
                    == blob["nrows"]
                )

def test_seed_sqlite(dataset):
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]
    config_path = os.path.join(data_dir, "config.ini")


    if munge:
        cytominer_database.munge.munge(config_path, data_dir)

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # create output directory
        sqlite_file = os.path.join(temp_dir, "test.db")
        target = "sqlite:///{}".format(str(sqlite_file))

        # run program
        cytominer_database.ingest_variable_engine.seed(
            config_path=config_path, source=data_dir, output_path=target, sqlite=True4
        )

        assert os.path.exists(str(sqlite_file))

        for blob in ingest:
            table_name = blob["table"].capitalize()
            engine = create_engine(target)
            con = engine.connect()
            df = pd.read_sql(sql=table_name, con=con)
            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert (
                    df.groupby(["TableNumber", "ImageNumber"]).size().sum()
                    == blob["nrows"]
                )

def test_seed_default(dataset):
    # Default of cytominer_database.ingest_variable_engine.seed(...) is sqlite=True and parquet=False
    # (defined in cytominer_database.get_engine())
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]
    config_path = os.path.join(data_dir, "config.ini")


    if munge:
        cytominer_database.munge.munge(config_path, data_dir)

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(temp_dir, "test.db")
        target = "sqlite:///{}".format(str(sqlite_file))

        # run program: sqlite is not specified!
        cytominer_database.ingest_variable_engine.seed(
            config_path=config_path, source=data_dir, output_path=target,
        )

        assert os.path.exists(str(sqlite_file))

        for blob in ingest:
            table_name = blob["table"].capitalize()
            engine = create_engine(target)
            con = engine.connect()
            df = pd.read_sql(sql=table_name, con=con)
            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert (
                    df.groupby(["TableNumber", "ImageNumber"]).size().sum()
                    == blob["nrows"]
                )


def test_seed_incompatible_engine(dataset):
    # tests if an error is thrown when both engines 'sqlite=True' and 'parquet=True'
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"] 
    config_path = os.path.join(data_dir, "config.ini")

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(temp_dir, "test_incomp.db")
        target = "sqlite:///{}".format(str(sqlite_file))
        # Make sure a ValueError is raised
        with pytest.raises('ValueError') as exc_info:
            cytominer_database.ingest_variable_engine.seed(
                config_path=config_path,
                source=data_dir,
                output_path=target,
                sqlite=True,
                parquet=True
                )
            assert exc_info.type is ValueError
            assert exc_info.value.args[0] == "Two command flags '--parquet' and '--sqlite' cannot be added simultaneously."
