import os

import click.testing
import configparser
import backports.tempfile
import odo
import pandas
import pytest

import cytominer_database.command


@pytest.fixture(scope="function")
def runner():
    return click.testing.CliRunner()


def test_help(runner):
    result = runner.invoke(cytominer_database.command.command, ["ingest", "--help"])

    assert "ingest [OPTIONS] SOURCE" in result.output


def test_run(dataset, runner):
    config_file = os.path.join(dataset["data_dir"], "config.ini")

    opts = [
        "ingest", dataset["data_dir"],
        "--config-file", config_file
    ]

    if dataset["munge"]:
        opts += ["--munge"]
    else:
        opts += ["--no-munge"]

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        sqlite_file = os.path.join(temp_dir, "test.db")

        opts += ["--target", "sqlite:///{}".format(sqlite_file)]

        result = runner.invoke(cytominer_database.command.command, opts)

        assert result.exit_code == 0

        config = configparser.ConfigParser()

        config.read(config_file)

        for (k, v) in dict({"cells": "Cells.csv", "cytoplasm": "Cytoplasm.csv", "nuclei": "Nuclei.csv"}).items():
            config["filenames"][k] = v

        for table_key in ["image", "cells", "cytoplasm", "nuclei"]:
            csv_filename = os.path.join(temp_dir, config["filenames"][table_key])

            table_name = config["filenames"][table_key].split(".")[0]

            odo.odo("sqlite:///{}::{}".format(str(sqlite_file), table_name), csv_filename)

            df = pandas.read_csv(csv_filename)

            assert df.shape[0] == dataset["ingest"]["{}_nrows".format(table_name)]

            assert df.shape[1] == dataset["ingest"]["{}_ncols".format(table_name)] + 1

            if table_key != "image":
                assert df.groupby(["TableNumber", "ImageNumber"]).size().sum() == \
                       dataset["ingest"]["{}_nrows".format(table_name)]