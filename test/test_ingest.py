"""

"""

import configparser
import glob
import os
import perturbation.ingest
import pytest
import subprocess


def test_seed(dataset, tmpdir):
    assert 1 == 1

    if dataset["munge"]:
        subprocess.call(["./munge.sh", dataset["data_dir"]])

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file)

    sqlite_file = tmpdir.mkdir("test").join("test.db")

    perturbation.ingest.seed(config=config, input=dataset["data_dir"], output="sqlite:///{}".format(str(sqlite_file)))
