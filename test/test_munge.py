"""

"""

import configparser
import odo
import os
import pandas as pd
import cytominer_database.munge
import tempfile


def test_munge(dataset):
    if not dataset["munge"]:
        return

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file)

    with tempfile.TemporaryDirectory() as temp_dir:
        valid_directories = cytominer_database.munge.munge(config=config, source=dataset["data_dir"], target=temp_dir)

        for directory in valid_directories:
            for csv_filename in ["Cells.csv",  "Cytoplasm.csv", "Nuclei.csv"]:
                input_csv = pd.read_csv(os.path.join(directory, csv_filename))

                output_csv = pd.read_csv(os.path.join(directory.replace(dataset["data_dir"],
                                                                        dataset["munged_dir"]),
                                                      csv_filename))

                assert (input_csv == output_csv).all().all()

