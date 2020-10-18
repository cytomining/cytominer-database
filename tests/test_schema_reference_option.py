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


@pytest.mark.parametrize(
    "ref_fraction,number_of_repetitions",
    [
        (0.5, 1),
        (0.6, 1),
        (0.8, 1),
        (1.0, 1),
        (0.5, 10),
        (0.6, 10),
        (0.8, 10),
        (1.0, 10),
        (0.5, 50),
        (0.6, 50),
        (0.8, 50),
        (1.0, 50),
    ],
)
def test_schema_ref_option_parquet(cellpainting, ref_fraction, number_of_repetitions):
    """
    This function tests for different options in the "schema" section
     of the config file. More speciifically, it compares the ingestion process
     in the case where the reference file is given explicitly as a path and
     the case where the reference file is determined after comparing the dimensions
     of a subset of all tables sampled uniformly at random. 
     
    This is tested only on data_b ("cellpainting") because the random sampling
     of the reference table relies on there being many files to sample from,
      most of which are complete (no missing columns or type imcompatibilities).

    """
    munge = cellpainting["munge"]
    ingest = cellpainting["ingest"]
    if munge:
        cytominer_database.munge.munge(path, data_dir)

    reference_dicts = {}
    reference_dicts["ref_sampling"] = get_reference_dicts(
        data=cellpainting,
        ref_fraction=ref_fraction,
        number_of_repetitions=number_of_repetitions,
    )  # path to config file with "reference_option = sample"
    reference_dicts["ref_chosen_file"] = get_reference_dicts(
        data=cellpainting
    )  # path to config file with "reference_option = path/to/reference_file"

    for blob in ingest:  # iterate over compartment types
        compartment_name = blob["table"]
        predetermined_schema = reference_dicts["ref_chosen_file"][
            compartment_name.capitalize()
        ]["schema"]
        #  get every sampled schema and compare it to predetermined schema
        for ref_dict in reference_dicts["ref_sampling"]:
            sampled_schemata = ref_dict[compartment_name.capitalize()]["schema"]
            assert sampled_schemata.types == predetermined_schema.types
            assert sampled_schemata.names == predetermined_schema.names


def get_reference_dicts(data, ref_fraction=None, number_of_repetitions=None):
    """
    Helper function used in test_schema_ref_option_parquet().
    Returns a list of n=number_of_repetitions reference dictionaries
    which were determined by sampling a subset of size ref_fraction. 
    If the optional parameters are not provided, the function returns a reference dictionary
    which is generated from the fixed reference tables stored under 'path/to/reference_tables',
    as specified in the configuration file. 
    """
    data_dir = data["data_dir"]
    if ref_fraction is not None:  # For the sampling-based approach:
        config_path = data["config"]
        config = cytominer_database.utils.read_config(config_path)
        # 1. Overwrite the sampling ration from the testing parametrization
        config["schema"]["ref_fraction"] = str(ref_fraction)
        list_of_repetitions = []
        for i in range(number_of_repetitions):
            # Build reference dictionary for that config
            list_of_repetitions.append(
                cytominer_database.tableSchema.get_ref_dict(
                    source=data_dir, config_file=config, skip_image_prefix=True
                )
            )
        return list_of_repetitions
    elif (ref_fraction is None) and (number_of_repetitions is None):
        # For the path-based approach (e.g. reference_option = "tests/data_b/A01-1")
        config_path = data["config_ref"]
        config = cytominer_database.utils.read_config(config_path)
        return cytominer_database.tableSchema.get_ref_dict(
            source=data_dir, config_file=config, skip_image_prefix=True
        )
    else:
        raise ValueError(
            "Incomplete values for the schema reference option 'reference_option = sampling' "
        )
