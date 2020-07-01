import os.path

import backports.tempfile
import cytominer_database.munge
import pandas as pd
import pandas.util.testing


def test_munge(dataset):
    if not dataset["munge"]:
        return

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    with backports.tempfile.TemporaryDirectory() as temp_dir:
        valid_directories = cytominer_database.munge.munge(
            config_path=config_file,
            source=dataset["data_dir"],
            target=temp_dir
        )

        for directory in valid_directories:
            for csv_filename in ["Cells.csv",  "Cytoplasm.csv", "Nuclei.csv"]:
                input_csv = pd.read_csv(
                    os.path.join(directory.replace(dataset["data_dir"], temp_dir), csv_filename)
                )

                output_csv = pd.read_csv(
                    os.path.join(directory.replace(dataset["data_dir"], dataset["munged_dir"]), csv_filename)
                )

                pandas.util.testing.assert_frame_equal(input_csv, output_csv)
