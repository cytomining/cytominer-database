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

docker_name = 'testdb_{:04d}'.format(random.randint(0, 9999))

@pytest.yield_fixture()
def session_postgres():

    cmd = 'PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "DROP DATABASE IF EXISTS testdb"'

    subprocess.check_output(cmd, shell=True)

    cmd = 'PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "CREATE DATABASE testdb"'

    subprocess.check_output(cmd, shell=True)

    engine = sqlalchemy.create_engine('postgresql://postgres:password@localhost:3210/testdb')

    session_postgres = sqlalchemy.orm.sessionmaker(bind=engine)

    perturbation.base.Base.metadata.create_all(engine)

    yield session_postgres()


def test_seed(session_postgres):
    subprocess.call(['./munge.sh', 'test/data'])

    config_file_sys = pkg_resources.resource_filename(pkg_resources.Requirement.parse("perturbation"), "config.ini")

    config = configparser.ConfigParser()

    config.read(config_file_sys)

    perturbation.database.seed(config, 'test/data', 'postgresql://postgres:password@localhost:3210/testdb', 'images', 'views.sql')

    for directory in glob.glob(os.path.join('test/data', '*/')):
        perturbation.database.seed(config, directory, 'postgresql://postgres:password@localhost:3210/testdb', 'objects', 'views.sql')

    n_plates = 1
    n_channels = 3
    n_patterns = 3
    n_wells = 4
    n_images = 8
    n_objects = 40
    n_bins_raddist = 4
    n_scales_texture = 3
    n_moments_coefs = 30

    n_matches = n_objects * n_patterns
    n_edges = n_matches * n_channels
    n_intensities = n_matches * n_channels
    n_textures = n_matches * n_channels * n_scales_texture
    n_radial_distributions = n_matches * n_channels * n_bins_raddist
    n_locations = n_matches * n_channels
    n_shapes = n_matches
    n_coordinates = n_matches + n_shapes + (n_matches * n_channels * 2)
    n_moments = n_shapes * n_moments_coefs
    n_neighborhoods = n_matches
    n_correlations = n_matches * 5

    assert len(session_postgres.query(perturbation.models.Pattern).all()) == n_patterns
    assert len(session_postgres.query(perturbation.models.Plate).all()) == n_plates
    assert len(session_postgres.query(perturbation.models.Channel).all()) == n_channels
    assert len(session_postgres.query(perturbation.models.Well).all()) == n_wells
    assert len(session_postgres.query(perturbation.models.Image).all()) == n_images
    assert len(session_postgres.query(perturbation.models.Match).all()) == n_matches
    assert len(session_postgres.query(perturbation.models.Edge).all()) == n_edges
    assert len(session_postgres.query(perturbation.models.Intensity).all()) == n_intensities
    assert len(session_postgres.query(perturbation.models.Texture).all()) == n_textures
    assert len(session_postgres.query(perturbation.models.RadialDistribution).all()) == n_radial_distributions
    assert len(session_postgres.query(perturbation.models.Shape).all()) == n_shapes
    assert len(session_postgres.query(perturbation.models.Location).all()) == n_locations
    assert len(session_postgres.query(perturbation.models.Coordinate).all()) == n_coordinates
    assert len(session_postgres.query(perturbation.models.Moment).all()) == n_moments
    assert len(session_postgres.query(perturbation.models.Neighborhood).all()) == n_neighborhoods
    assert len(session_postgres.query(perturbation.models.Correlation).all()) == n_correlations
    
    correlations = session_postgres.query(perturbation.models.Correlation)
    
    assert correlations.filter(perturbation.models.Correlation.match is None).all() == []
    
    assert len(session_postgres.query(sqlalchemy.Table('view_correlations', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_correlations
    assert len(session_postgres.query(sqlalchemy.Table('view_edges', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_edges
    assert len(session_postgres.query(sqlalchemy.Table('view_intensities', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_intensities
    assert len(session_postgres.query(sqlalchemy.Table('view_locations', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_locations
    assert len(session_postgres.query(sqlalchemy.Table('view_moments', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_moments
    assert len(session_postgres.query(sqlalchemy.Table('view_neighborhoods', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_neighborhoods
    assert len(session_postgres.query(sqlalchemy.Table('view_radial_distributions', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_radial_distributions
    assert len(session_postgres.query(sqlalchemy.Table('view_shapes', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_shapes
    assert len(session_postgres.query(sqlalchemy.Table('view_textures', perturbation.base.Base.metadata,
                                                       autoload_with=session_postgres.connection())).all()) == n_textures

