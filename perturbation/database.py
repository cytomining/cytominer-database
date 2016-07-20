import click
import glob
import hashlib
import os
import pandas
import collections
import logging
import perturbation.base
import perturbation.models
import perturbation.UUID
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.types
import uuid


logger = logging.getLogger(__name__)

Base = perturbation.base.Base

Session = sqlalchemy.orm.sessionmaker()

engine = None

scoped_session = sqlalchemy.orm.scoped_session(Session)

# initialize lists that will be used to store tables
channels = []
coordinates = []
correlations = []
edges = []
images = []
intensities = []
locations = []
qualities = []
matches = []
moments_group = []
neighborhoods = []
objects = []
plates = []
radial_distributions = []
shapes = []
textures = []
wells = []


def setup(connection):
    """Sets up SQLite database

    :param connection: name of SQLlite/PostGreSQL database
    :return: None
    """
    global engine

    engine = sqlalchemy.create_engine(connection)

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)


def seed(input, output, sqlfile=None):
    """Call functions to create backend

    :param input: top-level directory containing sub-directories, each of which have an image.csv and object.csv
    :param output: name of SQLlite/PostGreSQL database
    :param sqlfile: SQL file to be executed on the backend database after it is created
    :return:
    """
    setup(output)

    logger.debug('Parsing SQL file')

    if sqlfile: 
        create_views(sqlfile)

    logger.debug('Parsing csvs')

    seed_plate(input)


def seed_plate(directories):
    """Creates backend

    :param directories: top-level directory containing sub-directories, each of which have an image.csv and object.csv
    :return: None
    """
    pathnames = find_directories(directories)

    for directory in pathnames:
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        # TODO: 'Metadata_Barcode' should be gotten from a config file
        plate_descriptions = data['Metadata_Barcode'].unique()

        # Populate plates[], wells[], images[], qualities[]
        # This pre-computes UUIDs so that we don't need to look up the db
        # (which will be slow)
        
        create_plates(data, digest, plate_descriptions)

        # TODO: Read all the patterns (not just Cells.csv; note that some datasets may not have Cells as a pattern) 
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        # TODO: Avoid needing to explicity name all *ObjectNumber* columns
        object_numbers = pandas.concat([get_object_numbers(s) for s in ['ObjectNumber', 'Neighbors_FirstClosestObjectNumber_5', 'Neighbors_SecondClosestObjectNumber_5']])

        object_numbers.drop_duplicates()

        for index, object_number in object_numbers.iterrows():
            object_dictionary = create_object(digest, object_number)

            objects.append(object_dictionary)

        filenames = []

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [os.path.join(directory, 'image.csv'), os.path.join(directory, 'object.csv')]:
                filenames.append(os.path.basename(filename))

        pattern_descriptions = find_pattern_descriptions(filenames)

        patterns = find_patterns(pattern_descriptions, scoped_session)

        columns = data.columns

        find_channels(columns)

        correlation_columns = find_correlation_columns(columns)

        scales = find_scales(columns)

        counts = find_counts(columns)

        moments = find_moments(columns)

        # Populate all the tables
        create_patterns(correlation_columns, counts, digest, directory, moments, patterns, scales)

    __save__(perturbation.models.Channel, channels)

    __save__(perturbation.models.Plate, plates)


def create_patterns(correlation_columns, counts, digest, directory, moments, patterns, scales):
    """Populates all the tables in the backend

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

                # TODO: Avoid needing to explicitly name all *ObjectNumber* columns
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

                create_channels(counts, match, row, scales)

        logger.debug('\tCompleted parsing {}'.format(pattern.description))

    logger.debug('\tStarted committing {}'.format(os.path.basename(directory)))

    __save__(perturbation.models.Coordinate, coordinates)
    __save__(perturbation.models.Correlation, correlations)
    __save__(perturbation.models.Edge, edges)
    __save__(perturbation.models.Image, images)
    __save__(perturbation.models.Intensity, intensities)
    __save__(perturbation.models.Location, locations)
    __save__(perturbation.models.Match, matches)
    __save__(perturbation.models.Quality, qualities)
    __save__(perturbation.models.Well, wells)
    __save__(perturbation.models.Texture, textures)
    __save__(perturbation.models.Object, objects)
    __save__(perturbation.models.Neighborhood, neighborhoods)
    __save__(perturbation.models.Moment, moments_group)
    __save__(perturbation.models.Shape, shapes)
    __save__(perturbation.models.RadialDistribution, radial_distributions)

    logger.debug('\tCompleted committing {}'.format(os.path.basename(directory)))


def find_channels(columns):
    channel_descriptions = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Intensity":
            channel_description = split_columns[2]

            channel_descriptions.add(channel_description)

    for channel_description in channel_descriptions:
        channel = find_channel_by(channels, channel_description)

        if not channel:
            channel = create_channel(channel_description)

            channels.append(channel)


def find_correlation_columns(columns):
    correlation_columns = []

    for column in columns:
        split_columns = column.split("_")

        a = None
        b = None

        if split_columns[0] == "Correlation":
            for channel in channels:
                if channel["description"] == split_columns[2]:
                    a = channel

                if channel["description"] == split_columns[3]:
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


def find_directories(directory):
    directories = set()

    filenames = glob.glob(os.path.join(directory, '*'))

    for filename in filenames:
        pathname = os.path.relpath(filename)

        directories.add(pathname)

    return directories


def find_moments(columns):
    moments = []

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "AreaShape" and split_columns[1] == "Zernike":
            moment = (split_columns[2], split_columns[3])

            moments.append(moment)

    return moments


def find_pattern_descriptions(filenames):
    pattern_descriptions = []

    for filename in filenames:
        pattern_description = filename.split('.')[0]

        pattern_descriptions.append(pattern_description)

    return pattern_descriptions


def find_patterns(pattern_descriptions, session):
    patterns = []

    for pattern_description in pattern_descriptions:
        pattern = perturbation.models.Pattern.find_or_create_by(
                session=session,
                description=pattern_description
        )

        patterns.append(pattern)

    return patterns


def find_scales(columns):
    scales = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Texture":
            scale = split_columns[3]

            scales.add(scale)

    return scales


def create_channels(counts, match, row, scales):
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


def create_images(data, digest, descriptions, well):
    for description in descriptions:
        image = create_image(digest, description, well)

        images.append(image)

        quality = create_quality(data, description, image)

        qualities.append(quality)


def create_moments(moments, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


def create_plates(data, digest, descriptions):
    for description in descriptions:
        plate = find_plate_by(plates, str(description))

        if not plate:
            plate = create_plate(description)

            plates.append(plate)

        # TODO: 'Metadata_Barcode' should be gotten from a config file
        well_descriptions = data[data['Metadata_Barcode'] == description]['Metadata_Well'].unique()

        create_wells(data, digest, plate, description, well_descriptions)


def create_radial_distributions(channel, counts, match, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

        textures.append(texture)


def create_views(sqlfile):
    with open(sqlfile) as f:
        import sqlparse

        for s in sqlparse.split(f.read()):
            engine.execute(s)


def create_wells(data, digest, plate, plate_description, descriptions):
    for description in descriptions:
        well = create_well(plate, description)

        wells.append(well)

        # TODO: 'Metadata_Barcode' and 'Metadata_Well' should be gotten from a config file
        image_descriptions = data[(data['Metadata_Barcode'] == plate_description) & (data['Metadata_Well'] == description)]['ImageNumber'].unique()

        create_images(data, digest, image_descriptions, well)


def find_channel_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary["description"] == description:
            return dictionary["id"]


def find_image_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary["description"] == description:
            return dictionary["id"]


def find_object_by(description, dictionaries, image_id):
    for dictionary in dictionaries:
        if (dictionary["description"] == description) and (dictionary["image_id"] == image_id):
            return dictionary["id"]


def find_plate_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary["description"] == description:
            return dictionary


def create_channel(description):
    return {
        "description": description,
        "id": uuid.uuid4()
    }


def create_center(row):
    return {
            "abscissa": row['Location_Center_X'],
            "id": uuid.uuid4(),
            "ordinate": row['Location_Center_Y']
    }


def create_center_mass_intensity(channel, row):
    return {
            "abscissa": row['Location_CenterMassIntensity_X_{}'.format(channel["description"])],
            "id": uuid.uuid4(),
            "ordinate": row['Location_CenterMassIntensity_Y_{}'.format(channel["description"])]
    }


def create_correlation(dependent, independent, match, row):
    return {
            "coefficient": row['Correlation_Correlation_{}_{}'.format(dependent["description"], independent["description"])],
            "dependent_id": dependent["id"],
            "id": uuid.uuid4(),
            "independent_id": independent["id"],
            "match_id": match["id"]
    }


def create_edge(channel, match, row):
    return {
            "channel_id": channel["id"],
            "id": uuid.uuid4(),
            "integrated": row['Intensity_IntegratedIntensityEdge_{}'.format(channel["description"])],
            "match_id": match["id"],
            "maximum": row['Intensity_MaxIntensityEdge_{}'.format(channel["description"])],
            "mean": row['Intensity_MeanIntensityEdge_{}'.format(channel["description"])],
            "minimum": row['Intensity_MinIntensityEdge_{}'.format(channel["description"])],
            "standard_deviation": row['Intensity_StdIntensityEdge_{}'.format(channel["description"])]
    }


def create_max_intensity(channel, row):
    return {
            "abscissa": row['Location_MaxIntensity_X_{}'.format(channel["description"])],
            "id": uuid.uuid4(),
            "ordinate": row['Location_MaxIntensity_Y_{}'.format(channel["description"])]
    }


def create_image(digest, description, well_dictionary):
    return {
            "description": '{}_{}'.format(digest, int(description)),
            "id": uuid.uuid4(),
            "well_id": well_dictionary["id"]
    }


def create_intensity(channel, match, row):
    return {
            "channel_id": channel["id"],
            "first_quartile": row['Intensity_LowerQuartileIntensity_{}'.format(channel["description"])],
            "id": uuid.uuid4(),
            "integrated": row['Intensity_IntegratedIntensity_{}'.format(channel["description"])],
            "mass_displacement": row['Intensity_MassDisplacement_{}'.format(channel["description"])],
            "match_id": match["id"],
            "maximum": row['Intensity_MaxIntensity_{}'.format(channel["description"])],
            "mean": row['Intensity_MeanIntensity_{}'.format(channel["description"])],
            "median": row['Intensity_MedianIntensity_{}'.format(channel["description"])],
            "median_absolute_deviation": row['Intensity_MADIntensity_{}'.format(channel["description"])],
            "minimum": row['Intensity_MinIntensity_{}'.format(channel["description"])],
            "standard_deviation": row['Intensity_StdIntensity_{}'.format(channel["description"])],
            "third_quartile": row['Intensity_UpperQuartileIntensity_{}'.format(channel["description"])]
    }


def create_location(center_mass_intensity, channel, match, max_intensity):
    return {
            "center_mass_intensity_id": center_mass_intensity["id"],
            "channel_id": channel["id"],
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


def create_object(digest, description):
    return {
            "description": str(description['ObjectNumber']),
            "id": uuid.uuid4(),
            "image_id": find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])), dictionaries=images)
    }


def create_plate(description):
    return {
            "description": str(description),
            "id": uuid.uuid4()
    }


def create_quality(data, image_description, image):
    # TODO: 'Metadata_*' should be gotten from a config file
    return {
            "id": uuid.uuid4(),
            "image_id": image["id"],
            "count_cell_clump": int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isCellClump']),
            "count_debris": int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isDebris']),
            "count_low_intensity": int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isLowIntensity'])
    }


def create_radial_distribution(channel, count, match, row):
    return {
            "bins": count,
            "channel_id": channel["id"],
            "frac_at_d": row['RadialDistribution_FracAtD_{}_{}of4'.format(channel["description"], count)],
            "id": uuid.uuid4(),
            "match_id": match["id"],
            "mean_frac": row['RadialDistribution_MeanFrac_{}_{}of4'.format(channel["description"], count)],
            "radial_cv": row['RadialDistribution_RadialCV_{}_{}of4'.format(channel["description"], count)]
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
        return row[
            'Texture_{}_{}_{}_0'.format(
                    key,
                    channel["description"],
                    scale
            )
        ]

    return {
            "angular_second_moment": find_by('AngularSecondMoment'),
            "channel_id": channel["id"],
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


def create_well(plate_dictionary, well_description):
    return {
            "description": well_description,
            "id": uuid.uuid4(),
            "plate_id": plate_dictionary["id"]
    }


def __save__(table, records):
    """ Save records to table

    :param table: table class
    :param records: records to insert in the table
    :return:
    """
    logger.debug('\tStarted saving: {}'.format(str(table.__tablename__)))

    logger.debug('\t\tlen(records) = {}'.format(len(records)))

    logger.debug('\t\tInserting')

    scoped_session.execute(table.__table__.insert(), records)

    logger.debug('\t\tCommiting')

    scoped_session.commit()

    logger.debug('\t\tClearing')

    records.clear()

    logger.debug('\tCompleted saving: {}'.format(str(table.__tablename__)))
