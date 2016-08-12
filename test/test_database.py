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
 

def test_seed(dataset, session):

    munge_file = pkg_resources.resource_filename(__name__, "../perturbation/scripts/munge.sh")
    sql_file_sys = pkg_resources.resource_filename(__name__, "../perturbation/config/views.sql")

    if dataset["munge"]:
        subprocess.call([munge_file, dataset["data_dir"]])

    config_file = os.path.join(dataset["data_dir"], "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file)

    perturbation.database.seed(config=config, input=dataset["data_dir"], session=session, stage="images", sqlfile=sql_file_sys)

    for directory in glob.glob(os.path.join(dataset["data_dir"], "*/")):
        perturbation.database.seed(config=config, input=directory, session=session, stage="objects", sqlfile=sql_file_sys)

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

