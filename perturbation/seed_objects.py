"""

"""

import click
import collections
import hashlib
import logging
import os
import pandas
import perturbation.models
import perturbation.utils
import uuid

logger = logging.getLogger(__name__)

# initialize lists that will be used to store tables
coordinates = []
correlations = []
edges = []
intensities = []
locations = []
matches = []
moments_group = []
neighborhoods = []
objects = []
radial_distributions = []
shapes = []
textures = []


def seed(config, directory, scoped_session):
    """Creates backend

    :param config:
    :param directory: directory containing an image.csv and object.csv
    :param scoped_session:

    :return: None

    """
    try:
        _, image_csv = perturbation.utils.validate_csv_set(config, directory)

    except OSError as e:
        logger.warning(e)

        return

    data = pandas.read_csv(image_csv)

    logger.debug('Parsing {}'.format(directory))

    digest = hashlib.md5(open(os.path.join(directory, config['filenames']['image']), 'rb').read()).hexdigest()

    data = pandas.read_csv(os.path.join(directory, config['filenames']['reference_pattern']))

    columns = data.columns

    def get_object_numbers(s):
        return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

    neighborhood_scales = find_neighborhood_scales(columns)

    object_columns = ['ObjectNumber']

    for neighborhood_scale in neighborhood_scales:
        object_columns.extend(['Neighbors_FirstClosestObjectNumber_{}'.format(neighborhood_scale), 'Neighbors_SecondClosestObjectNumber_{}'.format(neighborhood_scale)])

    object_numbers = pandas.concat([get_object_numbers(s) for s in object_columns])

    object_numbers = object_numbers.drop_duplicates().reset_index()

    for index, object_number in object_numbers.iterrows():
        object_dictionary = create_object(digest, object_number, scoped_session)

        objects.append(object_dictionary)

    patterns = scoped_session.query(perturbation.models.Pattern).all()

    channels = scoped_session.query(perturbation.models.Channel).all()

    correlation_columns = find_correlation_columns(channels, columns)

    counts = find_counts(columns)

    moments = find_moments(columns)

    texture_scales = find_texture_scales(columns)

    # Populate all the tables
    create_patterns(channels, config, correlation_columns, counts, digest, directory, moments, neighborhood_scales, patterns, texture_scales, scoped_session)


def create_patterns(channels, config, correlation_columns, counts, digest, directory, moments, neighborhood_scales, patterns, texture_scales, session):
    """
    Populates all the tables in the backend

    :param channels:
    :param config:
    :param correlation_columns:
    :param counts:
    :param digest:
    :param directory:
    :param moments:
    :param neighborhood_scales:
    :param patterns:
    :param texture_scales:
    :param session:

    :return:

    """

    logger.debug('Reading {}'.format(os.path.basename(directory)))

    for pattern in patterns:
        logger.debug('\tStarted parsing {}'.format(pattern.description))

        data = pandas.read_csv(os.path.join(directory, '{}.csv').format(pattern.description))

        with click.progressbar(length=data.shape[0], label="Processing " + pattern.description, show_eta=True) as bar:
            for index, row in data.iterrows():
                bar.update(1)

                row = collections.defaultdict(lambda: None, row)

                image_id = find_image_by(description='{}_{}'.format(digest, int(row['ImageNumber'])), session=session)

                object_id = find_object_by(description=str(int(row['ObjectNumber'])), image_id=image_id, dictionaries=objects)

                center = create_center(row)

                coordinates.append(center)

                create_neighborhoods(image_id, object_id, row, neighborhood_scales)

                shape_center = create_shape_center(row)

                coordinates.append(shape_center)

                shape = create_shape(row, shape_center)

                shapes.append(shape)

                create_moments(moments, row, shape)

                match = create_match(center, object_id, pattern, shape)

                matches.append(match)

                create_correlations(correlation_columns, match, row)

                create_channels(channels, counts, match, row, texture_scales)

        logger.debug('\tCompleted parsing {}'.format(pattern.description))

    logger.debug('\tStarted committing {}'.format(os.path.basename(directory)))

    __save__(perturbation.models.Object, objects, session)
    __save__(perturbation.models.Coordinate, coordinates, session)
    __save__(perturbation.models.Neighborhood, neighborhoods, session)
    __save__(perturbation.models.Shape, shapes, session)
    __save__(perturbation.models.Moment, moments_group, session)
    __save__(perturbation.models.Match, matches, session)
    __save__(perturbation.models.Correlation, correlations, session)
    __save__(perturbation.models.Edge, edges, session)
    __save__(perturbation.models.Intensity, intensities, session)
    __save__(perturbation.models.Location, locations, session)
    __save__(perturbation.models.Texture, textures, session)
    __save__(perturbation.models.RadialDistribution, radial_distributions, session)

    logger.debug('\tCompleted committing {}'.format(os.path.basename(directory)))


def create_channels(channels, counts, match, row, texture_scales):
    for channel in channels:
        intensity = create_intensity(channel, match, row)

        intensities.append(intensity)

        edge = create_edge(channel, match, row)

        edges.append(edge)

        center_mass_intensity = create_center_mass_intensity(channel, row)

        coordinates.append(center_mass_intensity)

        max_intensity = create_max_intensity(channel, row)

        coordinates.append(max_intensity)

        location = create_location(center_mass_intensity, channel, match, max_intensity)

        locations.append(location)

        create_textures(channel, match, row, texture_scales)

        create_radial_distributions(channel, counts, match, row)


def create_correlations(correlation_columns, match, row):
    for dependent, independent in correlation_columns:
        correlation = create_correlation(dependent, independent, match, row)

        correlations.append(correlation)


def create_moments(moments, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


def create_neighborhoods(image_id, object_id, row, scales):
    for scale in scales:
        neighborhood = create_neighborhood(object_id, row, scale)

        if neighborhood is None:
            continue

        if row['Neighbors_FirstClosestObjectNumber_{}'.format(scale)]:
            description = str(int(row['Neighbors_FirstClosestObjectNumber_{}'.format(scale)]))

            first_closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

            neighborhood.update(closest_id=first_closest_id)

        if row['Neighbors_SecondClosestObjectNumber_{}'.format(scale)]:
            description = str(int(row['Neighbors_SecondClosestObjectNumber_{}'.format(scale)]))

            second_closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

            neighborhood.update(second_closest_id=second_closest_id)

        neighborhoods.append(neighborhood)


def create_radial_distributions(channel, counts, match, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        if radial_distribution is None:
            continue

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

        if texture is None:
            continue

        textures.append(texture)


def create_center(row):
    return {
            "abscissa": row['Location_Center_X'],
            "id": uuid.uuid4(),
            "ordinate": row['Location_Center_Y']
    }


def create_center_mass_intensity(channel, row):
    def find_by(key):
        return row[
            'Location_CenterMassIntensity_{}_{}'.format(
                    key,
                    channel.description
                    )
        ]
    return {
            "abscissa": find_by('X'),
            "id": uuid.uuid4(),
            "ordinate": find_by('Y')
    }


def create_correlation(dependent, independent, match, row):
    return {
            "coefficient": row['Correlation_Correlation_{}_{}'.format(dependent.description, independent.description)],
            "dependent_id": dependent.id,
            "id": uuid.uuid4(),
            "independent_id": independent.id,
            "match_id": match["id"]
    }


def create_edge(channel, match, row):
    def find_by(key):
        return row[
            'Intensity_{}Edge_{}'.format(
                    key,
                    channel.description
                    )
        ]
    return {
            "channel_id": channel.id,
            "id": uuid.uuid4(),
            "integrated": find_by('IntegratedIntensity'),
            "match_id": match["id"],
            "maximum": find_by('MaxIntensity'),
            "mean": find_by('MeanIntensity'),
            "minimum": find_by('MinIntensity'),
            "standard_deviation": find_by('StdIntensity')
    }


def create_max_intensity(channel, row):
    def find_by(key):
        return row[
            'Location_MaxIntensity_{}_{}'.format(
                    key,
                    channel.description
                    )
        ]
    return {
            "abscissa": find_by('X'),
            "id": uuid.uuid4(),
            "ordinate": find_by('Y')
    }


def create_intensity(channel, match, row):
    def find_by(key):
        return row[
            'Intensity_{}_{}'.format(
                    key,
                    channel.description
                    )
        ]
    return {
            "channel_id": channel.id,
            "first_quartile": find_by('LowerQuartileIntensity'),
            "id": uuid.uuid4(),
            "integrated": find_by('IntegratedIntensity'),
            "mass_displacement": find_by('MassDisplacement'),
            "match_id": match["id"],
            "maximum": find_by('MaxIntensity'),
            "mean": find_by('MeanIntensity'),
            "median": find_by('MedianIntensity'),
            "median_absolute_deviation": find_by('MADIntensity'),
            "minimum": find_by('MinIntensity'),
            "standard_deviation": find_by('StdIntensity'),
            "third_quartile": find_by('UpperQuartileIntensity')
    }


def create_location(center_mass_intensity, channel, match, max_intensity):
    return {
            "center_mass_intensity_id": center_mass_intensity["id"],
            "channel_id": channel.id,
            "id": uuid.uuid4(),
            "match_id": match["id"],
            "max_intensity_id": max_intensity["id"]
    }


def create_match(center, object_id, pattern, shape):
    return {
            "center_id": center["id"],
            "id": uuid.uuid4(),
            "object_id": object_id,
            "pattern_id": pattern.id,
            "shape_id": shape["id"]
    }


def create_moment(a, b, row, shape):
    return {
            "a": a,
            "b": b,
            "id": uuid.uuid4(),
            "score": row['AreaShape_Zernike_{}_{}'.format(a, b)],
            "shape_id": shape["id"]
    }


def create_neighborhood(object_id, row, scale):
    def find_by(key):
        template = 'Neighbors_{}_{}'
        return row[
            template.format(
                    key,
                    scale
            )
        ]
    if find_by('AngleBetweenNeighbors') is None:
        return None
    else:
        return {
                "angle_between_neighbors": find_by('AngleBetweenNeighbors'),
                "first_closest_id": None,
                "first_closest_distance": find_by('FirstClosestDistance'),
                "first_closest_object_number": find_by('FirstClosestObjectNumber'),
                "id": uuid.uuid4(),
                "number_of_neighbors": find_by('NumberOfNeighbors'),
                "object_id": object_id,
                "percent_touching": find_by('PercentTouching'),
                "scale": scale,
                "second_closest_distance" : find_by('SecondClosestDistance'),
                "second_closest_id": None,
                "second_closest_object_number": find_by('SecondClosestObjectNumber')
        }


def create_object(digest, description, session):
    return {
            "description": str(description['ObjectNumber']),
            "id": uuid.uuid4(),
            "image_id": find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])),
                                      session=session)
    }


def create_radial_distribution(channel, count, match, row):
    def find_by(key):
        return row[
            'RadialDistribution_{}_{}_{}of4'.format(
                    key,
                    channel.description,
                    count
            )
        ]
    if find_by('FracAtD') is None:
        return None
    else:
        return {
                "bins": count,
                "channel_id": channel.id,
                "frac_at_d": find_by("FracAtD"),
                "id": uuid.uuid4(),
                "match_id": match["id"],
                "mean_frac": find_by("MeanFrac"),
                "radial_cv": find_by("RadialCV")
        }


def create_shape(row, shape_center):
    return {
            "area": row['AreaShape_Area'],
            "center_id": shape_center["id"],
            "compactness": row['AreaShape_Compactness'],
            "eccentricity": row['AreaShape_Eccentricity'],
            "euler_number": row['AreaShape_EulerNumber'],
            "extent": row['AreaShape_Extent'],
            "form_factor": row['AreaShape_FormFactor'],
            "id": uuid.uuid4(),
            "major_axis_length": row['AreaShape_MajorAxisLength'],
            "max_feret_diameter": row['AreaShape_MaxFeretDiameter'],
            "maximum_radius": row['AreaShape_MaximumRadius'],
            "mean_radius": row['AreaShape_MeanRadius'],
            "median_radius": row['AreaShape_MedianRadius'],
            "min_feret_diameter": row['AreaShape_MinFeretDiameter'],
            "minor_axis_length": row['AreaShape_MinorAxisLength'],
            "orientation": row['AreaShape_Orientation'],
            "perimeter": row['AreaShape_Perimeter'],
            "solidity": row['AreaShape_Solidity']
    }


def create_shape_center(row):
    return {
            "abscissa": row['AreaShape_Center_X'],
            "id": uuid.uuid4(),
            "ordinate": row['AreaShape_Center_Y']
    }


def create_texture(channel, match, row, scale):
    def find_by(key):
        template = 'Texture_{}_{}_{}' if key == "Gabor" else 'Texture_{}_{}_{}_0'
        return row[
            template.format(
                    key,
                    channel.description,
                    scale
            )
        ]

    if find_by('AngularSecondMoment') is None:
        return None
    else:
        return {
                "angular_second_moment": find_by('AngularSecondMoment'),
                "channel_id": channel.id,
                "contrast": find_by('Contrast'),
                "correlation": find_by('Correlation'),
                "difference_entropy": find_by('DifferenceEntropy'),
                "difference_variance": find_by('DifferenceVariance'),
                "match_id": match["id"],
                "scale": scale,
                "entropy": find_by('Entropy'),
                "gabor": find_by('Gabor'),
                "id": uuid.uuid4(),
                "info_meas_1": find_by('InfoMeas1'),
                "info_meas_2": find_by('InfoMeas2'),
                "inverse_difference_moment": find_by('InverseDifferenceMoment'),
                "sum_average": find_by('SumAverage'),
                "sum_entropy": find_by('SumEntropy'),
                "sum_variance": find_by('SumVariance'),
                "variance": find_by('Variance')
        }


def find_correlation_columns(channels, columns):
    correlation_columns = []

    for column in columns:
        split_columns = column.split("_")

        a = None
        b = None

        # TODO: This excludes RWC, K, Overlap, Manders
        if split_columns[0] == "Correlation" and split_columns[1] == "Correlation":
            for channel in channels:
                if channel.description == split_columns[2]:
                    a = channel

                if channel.description == split_columns[3]:
                    b = channel

            correlation_column = (a, b)

            correlation_columns.append(correlation_column)

    return correlation_columns


def find_counts(columns):
    counts = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "RadialDistribution":
            count = split_columns[3].split('of')[0]

            counts.add(count)

    return counts


def find_moments(columns):
    moments = []

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "AreaShape" and split_columns[1] == "Zernike":
            moment = (split_columns[2], split_columns[3])

            moments.append(moment)

    return moments


def find_texture_scales(columns):
    scales = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Texture":
            scale = split_columns[3]

            scales.add(scale)

    return scales


def find_neighborhood_scales(columns):
    scales = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Neighbors":
            scale = split_columns[2]

            scales.add(scale)

    return scales


def find_object_by(description, dictionaries, image_id):
    for dictionary in dictionaries:
        if (dictionary["description"] == description) and (dictionary["image_id"] == image_id):
            return dictionary["id"]


def find_image_by(description, session):
    return session.query(perturbation.models.Image).filter(perturbation.models.Image.description == description).one().id


def __save__(table, records, session):
    """ Save records to table

    :param table: table class
    :param records: records to insert in the table
    :param session: session

    :return:

    """
    logger.debug('\tStarted saving: {}'.format(str(table.__tablename__)))

    logger.debug('\t\tlen(records) = {}'.format(len(records)))

    logger.debug('\t\tInserting')

    # http://docs.sqlalchemy.org/en/latest/_modules/examples/performance/bulk_inserts.html

    #session.bulk_insert_mappings(table, records)

    session.execute(table.__table__.insert(), records)

    logger.debug('\t\tCommitting')

    session.commit()

    logger.debug('\t\tClearing')

    records.clear()

    logger.debug('\tCompleted saving: {}'.format(str(table.__tablename__)))