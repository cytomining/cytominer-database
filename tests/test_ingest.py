import os

import backports.tempfile
import cytominer_database.ingest
import cytominer_database.munge
import odo
import pandas as pd


def test_seed(dataset):
    config_file = os.path.join(dataset["data_dir"], "config.ini")

    if dataset["munge"]:
        cytominer_database.munge.munge(config_file, dataset["data_dir"])

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(temp_dir, "test.db")

        cytominer_database.ingest.seed(
            config_file=config_file,
            source=dataset["data_dir"],
            target="sqlite:///{}".format(str(sqlite_file))
        )

        for blob in dataset["ingest"]:
            table_name = blob["table"]

            csv_pathname = os.path.join(temp_dir, "{}.csv".format(table_name))

            odo.odo("sqlite:///{}::{}".format(str(sqlite_file), table_name), csv_pathname)

            df = pd.read_csv(csv_pathname)

            assert df.shape[0] == blob["nrows"]

            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert df.groupby(["TableNumber", "ImageNumber"]).size().sum() == blob["nrows"]
