import os

import pytest
import pandas as pd
import click.testing
import backports.tempfile
from sqlalchemy import create_engine

import cytominer_database.command


@pytest.fixture(scope="function")
def runner():
    return click.testing.CliRunner()


def test_help(runner):
    result = runner.invoke(cytominer_database.command.command, ["ingest", "--help"])

    assert "ingest [OPTIONS] SOURCE TARGET" in result.output


def test_run(dataset, runner):
    opts = ["ingest"]

    if dataset["config"]:
        opts += ["--config-file", os.path.join(os.path.abspath(dataset["data_dir"]), dataset["config"])]

    if dataset["munge"]:
        opts += ["--munge"]
    else:
        opts += ["--no-munge"]

    opts += [os.path.abspath(dataset["data_dir"])]

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(os.path.abspath(temp_dir), "test.db")

        opts += ["sqlite:///{}".format(sqlite_file)]

        result = runner.invoke(cytominer_database.command.command, opts)

        assert result.exit_code == 0, result.output

        for blob in dataset["ingest"]:
            table_name = blob["table"].capitalize()

            target = "sqlite:///{}::{}".format(str(sqlite_file), table_name)
            engine = create_engine(target)
            con = engine.connect()

            df = pd.read_sql(target, con=con, index_col=0)

            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert df.groupby(["TableNumber", "ImageNumber"]).size().sum() == blob["nrows"]


def test_run_defaults(cellpainting, runner):
    opts = ["ingest"]

    if cellpainting["munge"]:
        opts += ["--munge"]
    else:
        opts += ["--no-munge"]

    opts += [os.path.abspath(cellpainting["data_dir"])]

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(os.path.abspath(temp_dir), "test.db")

        opts += ["sqlite:///{}".format(sqlite_file)]

        result = runner.invoke(cytominer_database.command.command, opts)

        assert result.exit_code == 0

        for blob in cellpainting["ingest"]:
            table_name = blob["table"].capitalize()

            target = "sqlite:///{}::{}".format(str(sqlite_file), table_name)
            engine = create_engine(target)
            con = engine.connect()

            df = pd.read_sql(target, con=con, index_col=0)

            assert df.shape[0] == blob["nrows"]
            assert df.shape[1] == blob["ncols"] + 1

            if table_name.lower() != "image":
                assert df.groupby(["TableNumber", "ImageNumber"]).size().sum() == blob["nrows"]
