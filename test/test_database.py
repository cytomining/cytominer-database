"""

"""

import configparser
import glob
import os
import pytest
import sqlalchemy
import sqlalchemy.orm
import perturbation.base
import perturbation.database
import perturbation.models
import pkg_resources
import random
import subprocess
 

datasets = {
    "htqc" : 
    {
    "data_dir" : "test/data_a",
    "row_counts" :
        {
            "n_plates" : 1,
            "n_channels" : 3,
            "n_channels_raddist" : 3,
            "n_patterns" : 3,
            "n_wells" : 4,
            "n_images" : 8,
            "n_objects" : 40,
            "n_bins_raddist" : 4,
            "n_scales_texture" : 3,
            "n_scales_neighborhood" : 2,
            "n_moments_coefs" : 30,
            "n_correlation_pairs" : 5
        },
    "munge" : True
    },
    "cellpainting" : 
    {
    "data_dir" : "test/data_b",
    "row_counts" :
        {
            "n_plates" : 1,
            "n_channels" : 5,
            "n_channels_raddist" : 4,
            "n_patterns" : 3,
            "n_wells" : 2,
            "n_images" : 4,
            "n_objects" : 40,
            "n_bins_raddist" : 4,
            "n_scales_texture" : 3,
            "n_scales_neighborhood" : 2,
            "n_moments_coefs" : 30,
            "n_correlation_pairs" : 10
        },
    "munge" : False
    }
}


def test_seed(selected_dataset, session):
    dataset = datasets[selected_dataset]

    if dataset["munge"]:
        subprocess.call(["./munge.sh", dataset["data_dir"]])

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file)

    connection_string = "postgresql://postgres:password@localhost:3210/testdb"

    perturbation.database.seed(config=config, input=dataset["data_dir"], output=connection_string, stage="images", sqlfile="views.sql")

    for directory in glob.glob(os.path.join(dataset["data_dir"], "*/")):
        perturbation.database.seed(config=config, input=directory, output=connection_string, stage="objects", sqlfile="views.sql")

    n_plates = dataset["row_counts"]["n_plates"]
    n_channels = dataset["row_counts"]["n_channels"]
    n_channels_raddist = dataset["row_counts"]["n_channels_raddist"]
    n_patterns = dataset["row_counts"]["n_patterns"]
    n_wells = dataset["row_counts"]["n_wells"]
    n_images = dataset["row_counts"]["n_images"]
    n_objects = dataset["row_counts"]["n_objects"]
    n_bins_raddist = dataset["row_counts"]["n_bins_raddist"]
    n_scales_texture = dataset["row_counts"]["n_scales_texture"]
    n_scales_neighborhood = dataset["row_counts"]["n_scales_neighborhood"]
    n_moments_coefs = dataset["row_counts"]["n_moments_coefs"]
    n_correlation_pairs = dataset["row_counts"]["n_correlation_pairs"]

    n_matches = n_objects * n_patterns
    n_edges = n_matches * n_channels
    n_intensities = n_matches * n_channels
    n_textures = n_matches * n_channels * n_scales_texture
    n_radial_distributions = n_matches * n_channels_raddist * n_bins_raddist
    n_locations = n_matches * n_channels
    n_shapes = n_matches
    n_coordinates = n_matches + n_shapes + (n_matches * n_channels * 2)
    n_moments = n_shapes * n_moments_coefs
    n_neighborhoods = n_objects * n_scales_neighborhood
    n_correlations = n_matches * n_correlation_pairs

    assert len(session.query(perturbation.models.Pattern).all()) == n_patterns
    assert len(session.query(perturbation.models.Plate).all()) == n_plates
    assert len(session.query(perturbation.models.Channel).all()) == n_channels
    assert len(session.query(perturbation.models.Well).all()) == n_wells
    assert len(session.query(perturbation.models.Image).all()) == n_images
    assert len(session.query(perturbation.models.Match).all()) == n_matches
    assert len(session.query(perturbation.models.Edge).all()) == n_edges
    assert len(session.query(perturbation.models.Intensity).all()) == n_intensities
    assert len(session.query(perturbation.models.Texture).all()) == n_textures
    assert len(session.query(perturbation.models.RadialDistribution).all()) == n_radial_distributions
    assert len(session.query(perturbation.models.Shape).all()) == n_shapes
    assert len(session.query(perturbation.models.Location).all()) == n_locations
    assert len(session.query(perturbation.models.Coordinate).all()) == n_coordinates
    assert len(session.query(perturbation.models.Moment).all()) == n_moments
    assert len(session.query(perturbation.models.Neighborhood).all()) == n_neighborhoods
    assert len(session.query(perturbation.models.Correlation).all()) == n_correlations
    
    correlations = session.query(perturbation.models.Correlation)
    
    assert correlations.filter(perturbation.models.Correlation.match is None).all() == []
    
    assert len(session.query(sqlalchemy.Table("view_correlations", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_correlations
    assert len(session.query(sqlalchemy.Table("view_edges", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_edges
    assert len(session.query(sqlalchemy.Table("view_intensities", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_intensities
    assert len(session.query(sqlalchemy.Table("view_locations", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_locations
    assert len(session.query(sqlalchemy.Table("view_moments", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_moments
    assert len(session.query(sqlalchemy.Table("view_neighborhoods", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_neighborhoods
    assert len(session.query(sqlalchemy.Table("view_radial_distributions", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_radial_distributions
    assert len(session.query(sqlalchemy.Table("view_shapes", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_shapes
    assert len(session.query(sqlalchemy.Table("view_textures", perturbation.base.Base.metadata,
                                                       autoload_with=session.connection())).all()) == n_textures

