"""

"""

import configparser
import odo
import os
import pandas as pd
import cytominer_database.ingest
import pkg_resources
import subprocess
import tempfile


def test_munge(dataset):
    if not dataset["munge"]:
        return

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file)

    with tempfile.TemporaryDirectory() as temp_dir:
        for (k, v) in dict({"cells": "Cells.csv", "cytoplasm": "Cytoplasm.csv", "nuclei": "Nuclei.csv"}).items():
            config["filenames"][k] = v

        for table_key in ["image", "cells", "cytoplasm", "nuclei"]:
            csv_filename = os.path.join(temp_dir, config["filenames"][table_key])

            df = pd.read_csv(csv_filename)

