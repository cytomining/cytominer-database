import os.path

import pandas as pd
import tempfile
from sqlalchemy import create_engine

import cytominer_database.ingest
import cytominer_database.munge
import warnings


def test_seed(dataset):
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]
    config = dataset["config"]
    if config:
        config_file = os.path.join(data_dir, config)
    else:
        config_file = os.path.join(data_dir, "config.ini")
        warnings.warn(
            "No config.ini file specified, using default config_file at {}".format(
                config_file
            ),
            UserWarning,
        )
    if munge:
        cytominer_database.munge.munge(config_file, data_dir)

    with tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(temp_dir, "test.db")

        cytominer_database.ingest.seed(
            config_path=config_file,
            source=data_dir,
            target="sqlite:///{}".format(str(sqlite_file)),
        )

        assert os.path.exists(str(sqlite_file))

        for blob in ingest:
            table_name = blob["table"].capitalize()
            target = "sqlite:///{}".format(str(sqlite_file))
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
