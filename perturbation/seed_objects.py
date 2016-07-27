import hashlib
import os
import pandas
import perturbation.models
import perturbation.utils
import logging
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


def seed(directories, scoped_session):
    """Creates backend

    :param directories: top-level directory containing sub-directories, each of which have an image.csv and object.csv
    :return: None
    """
    pathnames = perturbation.utils.find_directories(directories)

    for directory in pathnames:
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        # TODO: Read all the patterns (not just Cells.csv; note that some datasets may not have Cells as a pattern)
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        columns = data.columns

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        # TODO: Avoid explicitly naming all *ObjectNumber* columns
        object_numbers = pandas.concat([get_object_numbers(s) for s in ['ObjectNumber', 'Neighbors_FirstClosestObjectNumber_5', 'Neighbors_SecondClosestObjectNumber_5']])

        object_numbers.drop_duplicates()

        for index, object_number in object_numbers.iterrows():
            object_dictionary = create_object(digest, object_number, scoped_session)

            objects.append(object_dictionary)

        
        patterns = scoped_session.query(perturbation.models.Pattern).all()

        channels = scoped_session.query(perturbation.models.Channel).all()

        correlation_columns = find_correlation_columns(channels, columns)

        scales = find_scales(columns)

        counts = find_counts(columns)

        moments = find_moments(columns)

        # # Populate all the tables
        # create_patterns(channels, correlation_columns, counts, digest, directory, moments, patterns, scales)


    __save__(perturbation.models.Object, objects, scoped_session)


def create_patterns(channels, correlation_columns, counts, digest, directory, moments, patterns, scales):
    """Populates all the tables in the backend

    :param channels:
    :param correlation_columns:
    :param counts:
    :param digest:
    :param directory:
    :param moments:
    :param objects:
    :param patterns:
    :param scales:
    :return: None
    """
    logger.debug('Reading {}'.format(os.path.basename(directory)))

    for pattern in patterns:
        logger.debug('\tStarted parsing {}'.format(pattern.description))

        data = pandas.read_csv(os.path.join(directory, '{}.csv').format(pattern.description))

        with click.progressbar(length=data.shape[0], label="Processing " + pattern.description, show_eta=True) as bar:
            for index, row in data.iterrows():
                bar.update(1)

                row = collections.defaultdict(lambda: None, row)

                image_id = find_image_by(description='{}_{}'.format(digest, int(row['ImageNumber'])), dictionaries=images)

                object_id = find_object_by(description=str(int(row['ObjectNumber'])), image_id=image_id, dictionaries=objects)

                center = create_center(row)

                coordinates.append(center)

                neighborhood = create_neighborhood(object_id, row)

                # TODO: Avoid explicitly naming all *ObjectNumber* columns
                if row['Neighbors_FirstClosestObjectNumber_5']:
                    description = str(int(row['Neighbors_FirstClosestObjectNumber_5']))

                    closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

                    neighborhood.update(closest_id=closest_id)

                if row['Neighbors_SecondClosestObjectNumber_5']:
                    description = str(int(row['Neighbors_SecondClosestObjectNumber_5']))

                    second_closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

                    neighborhood.update(second_closest_id=second_closest_id)

                neighborhoods.append(neighborhood)

                shape_center = create_shape_center(row)

                coordinates.append(shape_center)

                shape = create_shape(row, shape_center)

                shapes.append(shape)

                create_moments(moments, row, shape)

                match = create_match(center, neighborhood, object_id, pattern, shape)

                matches.append(match)

                create_correlations(correlation_columns, match, row)

                create_channels(channels, counts, match, row, scales)

        logger.debug('\tCompleted parsing {}'.format(pattern.description))

    logger.debug('\tStarted committing {}'.format(os.path.basename(directory)))

    __save__(perturbation.models.Coordinate, coordinates)
    __save__(perturbation.models.Correlation, correlations)
    __save__(perturbation.models.Edge, edges)
    __save__(perturbation.models.Intensity, intensities)
    __save__(perturbation.models.Location, locations)
    __save__(perturbation.models.Match, matches)
    __save__(perturbation.models.Texture, textures)
    __save__(perturbation.models.Object, objects)
    __save__(perturbation.models.Neighborhood, neighborhoods)
    __save__(perturbation.models.Moment, moments_group)
    __save__(perturbation.models.Shape, shapes)
    __save__(perturbation.models.RadialDistribution, radial_distributions)

    logger.debug('\tCompleted committing {}'.format(os.path.basename(directory)))


def create_channels(channels, counts, match, row, scales):
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

        create_textures(channel, match, row, scales)

        create_radial_distributions(channel, counts, match, row)


def create_correlations(correlation_columns, match, row):
    for dependent, independent in correlation_columns:
        correlation = create_correlation(dependent, independent, match, row)

        correlations.append(correlation)


def create_moments(moments, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


def create_radial_distributions(channel, counts, match, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

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


def create_match(center, neighborhood, object_id, pattern, shape):
    return {
            "center_id": center["id"],
            "id": uuid.uuid4(),
            "neighborhood_id": neighborhood["id"],
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


def create_neighborhood(object_id, row):
    return {
            "angle_between_neighbors_5": row['Neighbors_AngleBetweenNeighbors_5'],
            "angle_between_neighbors_adjacent": row['Neighbors_AngleBetweenNeighbors_Adjacent'],
            "closest_id": None,
            "first_closest_distance_5": row['Neighbors_FirstClosestDistance_5'],
            "first_closest_distance_adjacent": row['Neighbors_FirstClosestDistance_Adjacent'],
            "first_closest_object_number_adjacent": row['Neighbors_FirstClosestObjectNumber_Adjacent'],
            "id": uuid.uuid4(),
            "number_of_neighbors_5": row['Neighbors_NumberOfNeighbors_5'],
            "number_of_neighbors_adjacent": row['Neighbors_NumberOfNeighbors_Adjacent'],
            "object_id": object_id,
            "percent_touching_5": row['Neighbors_PercentTouching_5'],
            "percent_touching_adjacent": row['Neighbors_PercentTouching_Adjacent'],
            "second_closest_distance_5": row['Neighbors_SecondClosestDistance_5'],
            "second_closest_distance_adjacent": row['Neighbors_SecondClosestDistance_Adjacent'],
            "second_closest_id": None,
            "second_closest_object_number_adjacent": row['Neighbors_SecondClosestObjectNumber_Adjacent']
    }


def create_object(digest, description, session):
    return {
            "description": str(description['ObjectNumber']),
            "id": uuid.uuid4(),
            "image_id": find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])),
                                      session=session).id
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

        if split_columns[0] == "Correlation":
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


def find_scales(columns):
    scales = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Texture":
            scale = split_columns[3]

            scales.add(scale)

    return scales


def find_object_by(description, dictionaries, image_id):
    for dictionary in dictionaries:
        if (dictionary["description"] == description) and (dictionary["image_id"] == image_id):
            return dictionary["id"]


def find_image_by(description, session):
    return session.query(perturbation.models.Image).filter(perturbation.models.Image.description == description).one()


def __save__(table, records, scoped_session):
    """ Save records to table

    :param table: table class
    :param records: records to insert in the table
    :return:
    """
    logger.debug('\tStarted saving: {}'.format(str(table.__tablename__)))

    logger.debug('\t\tlen(records) = {}'.format(len(records)))

    logger.debug('\t\tInserting')

    scoped_session.execute(table.__table__.insert(), records)

    logger.debug('\t\tCommitting')

    scoped_session.commit()

    logger.debug('\t\tClearing')

    records.clear()

    logger.debug('\tCompleted saving: {}'.format(str(table.__tablename__)))