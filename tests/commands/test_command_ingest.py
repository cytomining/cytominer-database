import os.path
import pandas as pd
import pytest
import click.testing
import backports.tempfile
from sqlalchemy import create_engine
import cytominer_database.commands.command_ingest_variable_engine


@pytest.fixture(scope="function")
def runner():
    return click.testing.CliRunner()

def test_run_variable_engine_sqlite(dataset, runner):
    CONFIG_CHOICE = "config.ini"
    # SOURCE
    opts = [dataset["data_dir"]]
    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # TARGET
        sqlite_file = os.path.join(temp_dir, "test_sqlite_output.db")
        opts += ["sqlite:///{}".format(sqlite_file)]
        # config_file
        opts += ["--config-file", os.path.join(dataset["data_dir"], CONFIG_CHOICE)]
        # munge
        if dataset["munge"]:
            opts += ["--munge"]
        else:
            opts += ["--no-munge"]
        # sqlite backend
        opts += ["--sqlite"]
        opts += ["--no-parquet"]
        # run command
        result = runner.invoke(
            cytominer_database.commands.command_ingest_variable_engine.command, opts
        )
        # test outcome
        assert result.exit_code == 0, result.output

        for blob in dataset["ingest"]:
            table_name = blob["table"].capitalize()

            target = "sqlite:///{}".format(str(sqlite_file))
            engine = create_engine(target)
            con = engine.connect()

            df = pd.read_sql(sql=table_name, con=con, index_col=0)

            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert (
                    df.groupby(["TableNumber", "ImageNumber"]).size().sum()
                    == blob["nrows"]
                )


def test_run_variable_engine_parquet(dataset, runner):
    CONFIG_CHOICE = "config.ini"
    # SOURCE
    opts = [dataset["data_dir"]]

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # TARGET
        # create output directory
        target = os.path.join(temp_dir, "test_parquet_output")
        try:
            os.stat(target)
        except:
            os.mkdir(target)
        opts += [target]

        # config_file
        opts += ["--config-file", os.path.join(dataset["data_dir"], CONFIG_CHOICE)]

        # munge
        if dataset["munge"]:
            opts += ["--munge"]
        else:
            opts += ["--no-munge"]
        # engine
        opts += ["--parquet"]
        opts += ["--no-sqlite"]

        # run command
        result = runner.invoke(
            cytominer_database.commands.command_ingest_variable_engine.command, opts, catch_exceptions=True
            # cytominer_database.commands.command_ingest_variable_engine.command, opts, catch_exceptions=False
        )
        assert result.exit_code == 0, result.output

        for blob in dataset["ingest"]:
            table_name = blob["table"].capitalize()

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

