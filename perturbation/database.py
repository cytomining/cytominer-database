import perturbation.models
import click
import collections
import glob
import hashlib
import os
import perturbation.base
import perturbation.migration.models
import pandas
import sqlalchemy
import sqlalchemy.orm
import logging

create_center = perturbation.migration.models.create_center

create_center_mass_intensity = perturbation.migration.models.create_center_mass_intensity

create_channel = perturbation.migration.models.create_channel

create_correlation = perturbation.migration.models.create_correlation

create_edge = perturbation.migration.models.create_edge

create_image = perturbation.migration.models.create_image

create_intensity = perturbation.migration.models.create_intensity

create_location = perturbation.migration.models.create_location

create_match = perturbation.migration.models.create_match

create_max_intensity = perturbation.migration.models.create_max_intensity

create_moment = perturbation.migration.models.create_moment

create_neighborhood = perturbation.migration.models.create_neighborhood

create_object = perturbation.migration.models.create_object

create_plate = perturbation.migration.models.create_plate

create_quality = perturbation.migration.models.create_quality

create_radial_distribution = perturbation.migration.models.create_radial_distribution

create_shape = perturbation.migration.models.create_shape

create_shape_center = perturbation.migration.models.create_shape_center

create_texture = perturbation.migration.models.create_texture

create_well = perturbation.migration.models.create_well

find_image_by = perturbation.migration.models.find_image_by

find_object_by = perturbation.migration.models.find_object_by

find_plate_by = perturbation.migration.models.find_plate_by

logger = logging.getLogger(__name__)

Base = perturbation.base.Base

Session = sqlalchemy.orm.sessionmaker()

scoped_session = sqlalchemy.orm.scoped_session(Session)


def find_directories(directory):
    directories = []

    filenames = glob.glob(os.path.join(directory, '*'))

    for filename in filenames:
        directories.append(os.path.relpath(filename))

    return set(directories)


def setup(database):
    global engine

    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(database)))

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)


def seed(input, output, sqlfile, verbose=False):
    setup(output)

    logger.debug('Parsing SQL file')

    with open(sqlfile) as f:
        import sqlparse
        for s in sqlparse.split(f.read()):
            engine.execute(s)

    channels = []

    plates = []

    correlation_offset = 0

    intensity_offset = 0

    location_offset = 0

    moment_offset = 0

    texture_offset = 0

    radial_distribution_offset = 0

    seed_plate(channels, correlation_offset, input, intensity_offset, location_offset, moment_offset, plates, radial_distribution_offset, texture_offset)


def seed_plate(channels, correlation_offset, input, intensity_offset, location_offset, moment_offset, plates, radial_distribution_offset, texture_offset):
    for directory in find_directories(input):
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        logger.debug('Parsing {}'.format(os.path.basename(directory)))

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

        radial_distributions = []

        shapes = []

        textures = []

        wells = []

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        plate_descriptions = data['Metadata_Barcode'].unique()

        logger.debug('\tParse plates, wells, images, quality')

        create_plates(data, digest, images, plate_descriptions, plates, qualities, wells)

        logger.debug('\tParse objects')

        # TODO: Read all the patterns because some columns are present in only one pattern
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        object_numbers = pandas.concat([get_object_numbers(s) for s in
                                        ['ObjectNumber', 'Neighbors_FirstClosestObjectNumber_5',
                                         'Neighbors_SecondClosestObjectNumber_5']])

        object_numbers.drop_duplicates()

        objects = find_objects(digest, images, object_numbers)

        scoped_session.commit()

        logger.debug('\tParse feature parameters')

        filenames = []

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [os.path.join(directory, 'image.csv'), os.path.join(directory, 'object.csv')]:
                filenames.append(os.path.basename(filename))

        pattern_descriptions = find_pattern_descriptions(filenames)

        patterns = find_patterns(pattern_descriptions, scoped_session)

        columns = data.columns

        find_channel_descriptions(channels, columns)

        correlation_columns = find_correlation_columns(channels, columns)

        scales = find_scales(columns)

        counts = find_counts(columns)

        moments = find_moments(columns)

        create_patterns(channels, coordinates, correlation_columns, correlation_offset, correlations, counts, digest, directory, edges, images, intensities, intensity_offset, location_offset, locations, matches, moment_offset, moments, moments_group, neighborhoods, objects, patterns, qualities, radial_distribution_offset, radial_distributions, scales, shapes, texture_offset, textures, wells)

    perturbation.migration.models.save_channels(channels, scoped_session)

    perturbation.migration.models.save_plates(plates, scoped_session)

    logger.debug('Commit plate, channel')

    scoped_session.commit()


def find_objects(digest, images, object_numbers):
    objects = []

    for index, object_number in object_numbers.iterrows():
        object_dictionary = create_object(digest, images, object_number)

        objects.append(object_dictionary)

    return objects


def create_patterns(channels, coordinates, correlation_columns, correlation_offset, correlations, counts, digest, directory, edges, images, intensities, intensity_offset, location_offset, locations, matches, moment_offset, moments, moments_group, neighborhoods, objects, patterns, qualities, radial_distribution_offset, radial_distributions, scales, shapes, texture_offset, textures, wells):
    for pattern in patterns:
        logger.debug('\tParse {}'.format(pattern.description))

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

                if row['Neighbors_FirstClosestObjectNumber_5']:
                    description = str(int(row['Neighbors_FirstClosestObjectNumber_5']))

                    closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

                    neighborhood._replace(closest_id=closest_id)

                if row['Neighbors_SecondClosestObjectNumber_5']:
                    description = str(int(row['Neighbors_SecondClosestObjectNumber_5']))

                    second_closest_id = find_object_by(description=description, image_id=image_id, dictionaries=objects)

                    neighborhood._replace(second_closest_id=second_closest_id)

                neighborhoods.append(neighborhood)

                shape_center = create_shape_center(row)

                coordinates.append(shape_center)

                shape = create_shape(row, shape_center)

                shapes.append(shape)

                create_moments(moments, moments_group, row, shape)

                match = create_match(center, neighborhood, object_id, pattern, shape)

                matches.append(match)

                create_correlations(correlation_columns, correlations, match, row)

                create_channels(channels, coordinates, counts, edges, intensities, locations, match, radial_distributions, row, scales, textures)

    perturbation.migration.models.save_coordinates(coordinates, scoped_session)
    perturbation.migration.models.save_edges(edges, scoped_session)
    perturbation.migration.models.save_images(images, scoped_session)
    perturbation.migration.models.save_matches(matches, scoped_session)
    perturbation.migration.models.save_neighborhoods(neighborhoods, scoped_session)
    perturbation.migration.models.save_objects(objects, scoped_session)
    perturbation.migration.models.save_qualities(qualities, scoped_session)
    perturbation.migration.models.save_shapes(scoped_session, shapes)
    perturbation.migration.models.save_textures(scoped_session, texture_offset, textures)
    perturbation.migration.models.save_wells(scoped_session, wells)
    perturbation.migration.models.save_correlations(correlation_offset, correlations, scoped_session)
    perturbation.migration.models.save_intensities(intensities, intensity_offset, scoped_session)
    perturbation.migration.models.save_locations(location_offset, locations, scoped_session)
    perturbation.migration.models.save_moments(moment_offset, moments, moments_group, scoped_session)
    perturbation.migration.models.save_radial_distributions(radial_distribution_offset, radial_distributions, scoped_session)

    logger.debug('\tCommit {}'.format(os.path.basename(directory)))

    scoped_session.commit()


def find_pattern_descriptions(filenames):
    pattern_descriptions = []

    for filename in filenames:
        pattern_descriptions.append(filename.split('.')[0])

    return pattern_descriptions


def find_patterns(pattern_descriptions, session):
    patterns = []

    for pattern_description in pattern_descriptions:
        pattern = perturbation.models.Pattern.find_or_create_by(session=session, description=pattern_description)

        patterns.append(pattern)

    return patterns


def find_channel_descriptions(channels, columns):
    channel_descriptions = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Intensity':
            channel_descriptions.append(split_columns[2])

    channel_descriptions = set(channel_descriptions)

    for channel_description in channel_descriptions:
        channel = perturbation.migration.models.find_channel_by(channels, channel_description)

        if not channel:
            channel = create_channel(channel_description, channel)

            channels.append(channel)


def find_moments(columns):
    moments = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'AreaShape' and split_columns[1] == 'Zernike':
            moments.append((split_columns[2], split_columns[3]))

    return moments


def find_counts(columns):
    counts = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'RadialDistribution':
            counts.append(split_columns[3].split('of')[0])

    counts = set(counts)

    return counts


def find_scales(columns):
    scales = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Texture':
            scales.append(split_columns[3])

    scales = set(scales)

    return scales


def find_correlation_columns(channels, columns):
    correlation_columns = []

    for column in columns:
        split_columns = column.split('_')

        a = None
        b = None

        if split_columns[0] == 'Correlation':
            for channel in channels:
                if channel.description == split_columns[2]:
                    a = channel

                if channel.description == split_columns[3]:
                    b = channel

            correlation_columns.append((a, b))

    return correlation_columns


def create_correlations(correlation_columns, correlations, match, row):
    for dependent, independent in correlation_columns:
        correlation = create_correlation(dependent, independent, match, row)

        correlations.append(correlation)


def create_moments(moments, moments_group, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


def create_channels(channels, coordinates, counts, edges, intensities, locations, match, radial_distributions, row, scales, textures):
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

        create_textures(channel, match, row, scales, textures)

        create_radial_distributions(channel, counts, match, radial_distributions, row)


def create_radial_distributions(channel, counts, match, radial_distributions, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales, textures):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

        textures.append(texture)


def create_images(data, digest, descriptions, images, qualities, well):
    for description in descriptions:
        image = create_image(digest, description, well)

        images.append(image)

        quality = create_quality(data, description, image)

        qualities.append(quality)


def create_plates(data, digest, images, descriptions, plates, qualities, wells):
    for description in descriptions:
        plate = find_plate_by(plates, str(int(description)))

        if not plate:
            plate = create_plate(description, plate)

            plates.append(plate)

        well_descriptions = data[data['Metadata_Barcode'] == description]['Metadata_Well'].unique()

        create_wells(data, digest, images, plate, description, qualities, well_descriptions, wells)


def create_wells(data, digest, images, plate, plate_description, qualities, descriptions, wells):
    for description in descriptions:
        well = create_well(plate, description)

        wells.append(well)

        image_descriptions = data[(data['Metadata_Barcode'] == plate_description) & (data['Metadata_Well'] == description)]['ImageNumber'].unique()

        create_images(data, digest, image_descriptions, images, qualities, well)
