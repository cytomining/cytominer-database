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
#!!!!!!!!!!!!! adapted config_new.ini !!!!!!!!!
    config_path = os.path.join(data_dir, "config.ini")
    # Get database engine option
    # (for this we need to read the config file from the path here already!)
    config_file = cytominer_database.utils.read_config(config_path)
    engine      = config_file["database_engine"]["database"]

    if munge:
        cytominer_database.munge.munge(config_path, data_dir)
        # Notes:
        # (1) Input agument "config_path" is unparsed (just a path string).
        # We could also just load the config file once and pass it along,
        # instead of passing the string and reloading the file several times
        # i.e. call cytominer_database.utils.read_config(config_path) only once.
        # (2) The function argument name in the definition was changed as follows:
        # cytominer_database.utils.read_config(config_file) ---> cytominer_database.utils.read_config(config_path)
        # because config_file in read_config(config_file) is not a file, but a path to a file.
        # The function documentation has been changed accordingly.

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


        cytominer_database.ingest_incl_parquet.seed(
            config_path=config_path,
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
