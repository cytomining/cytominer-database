import os

import pandas as pd
import backports.tempfile
from sqlalchemy import create_engine

import cytominer_database.ingest
import cytominer_database.munge


def test_seed(dataset):
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]

    config_file = os.path.join(data_dir, "config.ini")
    # moved upwards from lower level cytominer_database.ingest.seed()
    config_file = cytominer_database.utils.read_config(config_file)
    # get database engine option
    engine  = config_file["database_engine"]["database"]

    if munge:
        cytominer_database.munge.munge(config_file, data_dir)

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        if engine == 'Parquet':
            # create output directory
            target = os.path.join(temp_dir, "test_parquet_output" )
            try:
                os.stat(target)
            except:
                os.mkdir(target)

        elif engine == 'SQLite':
            sqlite_file = os.path.join(temp_dir, "test.db")
            target      = "sqlite:///{}".format(str(sqlite_file))


        cytominer_database.ingest.seed(
            config_file=config_file,
            source=data_dir,
            target=target
        )
        if engine == 'Parquet':
            assert os.path.exists(str(target))
        
        elif engine == 'SQLite':
            assert os.path.exists(str(sqlite_file))

        for blob in ingest:
            table_name = blob["table"].capitalize()
            
            if engine == 'Parquet':
                df = pd.read_parquet(path=target, engine ='pyarrow') # ignore column 0 (columns=[1:])? Column 0 should be read only as an index (index_col=0) ?
            
            elif engine == 'SQLite':
                target = "sqlite:///{}".format(str(sqlite_file))
                engine = create_engine(target)
                con = engine.connect()
                df = pd.read_sql(sql=table_name, con=con, index_col=0)

            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert df.groupby(["TableNumber", "ImageNumber"]).size().sum() == blob["nrows"]
