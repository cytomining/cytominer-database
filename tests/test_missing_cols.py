import pytest
import os
import pandas as pd
import backports.tempfile
from sqlalchemy import create_engine

import cytominer_database.ingest_variable_engine
import cytominer_database.munge
import cytominer_database.tableSchema
import pytest
import click
import pyarrow.parquet


def test_missing_cols_parquet(dataset):
    """
    This function will load the dataset as Pandas dataframe and
     immediately drop some columns in one of the datasets.
    It will then ingest the files to a Parquet backend,
     in which concatenate Tables of the same kind. 
    If one of the tables has a missing column, the column is added
     and padded with NaN as default in the alignement with the reference frame.
    The test checks if the dataframe is ingested even if a column is dropped and 
        if the padding was correct.
    Based on test_missing_columns_parquet(dataset).
    Run this only for data_b ("cellpainting") because the random sampling
     of the reference table relies on there being many files to sample from,
      most of which are complete (no missing columns or type imcompatibilities).

    """
    data_dir = dataset["data_dir"]
    munge = dataset["munge"]
    ingest = dataset["ingest"]
    config_path = dataset["config"]
    if config_path:
        config_path = os.path.join(data_dir, config_path)
    else:
        config_path = "config_default.ini"
    config_file = cytominer_database.utils.read_config(config_path)

    if munge:
        cytominer_database.munge.munge(config_path, data_dir)
    with backports.tempfile.TemporaryDirectory() as temp_dir:
        # create output directory
        target = os.path.join(temp_dir, "test_parquet_output")
        try:
            os.stat(target)
        except:
            os.mkdir(target)
        # run program
        cytominer_database.ingest_variable_engine.seed(
            config_path=config_path, source=data_dir, output_path=target, parquet=True
        )

        assert os.path.exists(str(target))
        # build reference dict
        reference_dict = cytominer_database.tableSchema.get_ref_dict(
            source=data_dir, config_file=config_file, skip_image_prefix=True
        )
        # get identifiers and build dictionary of processed Tables
        directories = sorted(list(cytominer_database.utils.find_directories(data_dir)))
        processed_tables = (
            {}
        )  # processed_tables[identifier][compartment] stores the modified dataframes loaded for comparison
        for directory in directories:
            # The test should compare only files which were not skipped during ingestion:
            if os.path.basename(directory) in dataset["skipped_dirs"]:
                continue
            image_csv = os.path.join(directory, dataset["image_csv"])
            identifier = cytominer_database.ingest_variable_engine.checksum(image_csv)
            processed_tables[identifier] = {}
            for blob in ingest:  # iterate over compartment types
                compartment_name = blob["table"]
                compartment_path = os.path.join(directory, compartment_name + ".csv")
                #  1) load. 2) modify: 2A) add 'TableNumber' column and 2B) optionally add prefix
                processed_df = cytominer_database.load.get_and_modify_df(
                    input=compartment_path,
                    identifier=identifier,
                    skip_image_prefix=True,
                )
                # 3) Alignment with reference table
                ref_dataframe = reference_dict[compartment_name.capitalize()][
                    "pandas_dataframe"
                ]
                aligned_and_processed_dataframe, _ = processed_df.align(
                    ref_dataframe, join="right", axis=1
                )
                # 4) Type conversion
                cytominer_database.utils.type_convert_dataframe(
                    dataframe=aligned_and_processed_dataframe, config=config_file
                )
                # 5) Store modified table in dictionary for comparison with written Parquet files
                processed_tables[identifier][
                    compartment_name
                ] = aligned_and_processed_dataframe
        # now: read written Parquet files
        for blob in ingest:
            compartment_name = blob["table"]
            # df = pd.read_parquet(path=target, engine ='pyarrow') # ignore column 0 (columns=[1:])? Column 0 should be read only as an index (index_col=0) ?
            basename = ".".join([compartment_name, "parquet"])
            compartment_path = os.path.join(target, basename)
            written_parquet = pyarrow.parquet.ParquetFile(compartment_path)
            # slice into groups
            sliced_df = slice_parquet_tables(written_parquet)
            for sliced in sliced_df:
                id = sliced.loc[0, "TableNumber"]
                # compare with corresponding file
                assert processed_tables[id][compartment_name].equals(sliced)


def slice_parquet_tables(written_parquet):
    """
    This function splits up the concatenated Parquet file (which was written to disk)
    and returns a list of Pandas dataframes.
    """
    sliced_df = []
    for i in range(written_parquet.num_row_groups):
        sliced_df.append(written_parquet.read_row_group(i).to_pandas())
    return sliced_df
