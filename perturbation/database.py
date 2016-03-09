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

logger = logging.getLogger(__name__)


def find_directories(directory):
    """

    :param directory:

    :return:

    """

    directories = []

    filenames = glob.glob(os.path.join(directory, '*'))

    for filename in filenames:
        directories.append(os.path.relpath(filename))

    return set(directories)


def seed(input, output, sqlfile, verbose=False):
    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

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

        objects = []

        radial_distributions = []

        shapes = []

        textures = []

        wells = []

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        plate_descriptions = data['Metadata_Barcode'].unique()

        logger.debug('\tParse plates, wells, images, quality')

        for plate_description in plate_descriptions:
            plate = perturbation.migration.models.find_plate_by(plates, str(int(plate_description)))

            if not plate:
                plate = perturbation.migration.models.create_plate(plate_description, plate)

                plates.append(plate)

            well_descriptions = data[data['Metadata_Barcode'] == plate_description]['Metadata_Well'].unique()

            for well_description in well_descriptions:
                well = perturbation.migration.models.create_well(plate, well_description)

                wells.append(well)

                image_descriptions = data[
                    (data['Metadata_Barcode'] == plate_description) & (data['Metadata_Well'] == well_description)
                ]['ImageNumber'].unique()

                for image_description in image_descriptions:
                    image = perturbation.migration.models.create_image(digest, image_description, well)

                    images.append(image)

                    quality = perturbation.migration.models.create_quality(data, image_description, image)

                    qualities.append(quality)

        logger.debug('\tParse objects')

        # TODO: Read all the patterns because some columns are present in only one pattern
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        object_numbers = pandas.concat(
                [get_object_numbers(s) for s in ['ObjectNumber',
                                                 'Neighbors_FirstClosestObjectNumber_5',
                                                 'Neighbors_SecondClosestObjectNumber_5'
                                                 ]]
        )

        object_numbers.drop_duplicates()

        for index, object_number in object_numbers.iterrows():
            object_dictionary = perturbation.migration.models.create_object(digest, images, object_number)

            objects.append(object_dictionary)

        session.commit()

        logger.debug('\tParse feature parameters')

        filenames = []

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [os.path.join(directory, 'image.csv'), os.path.join(directory, 'object.csv')]:
                filenames.append(os.path.basename(filename))

        pattern_descriptions = []

        for filename in filenames:
            pattern_descriptions.append(filename.split('.')[0])

        patterns = []

        for pattern_description in pattern_descriptions:
            pattern = perturbation.models.Pattern.find_or_create_by(
                    session=session,
                    description=pattern_description
            )

            patterns.append(pattern)

        columns = data.columns

        correlation_columns = []

        channel_descriptions = []

        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'Intensity':
                channel_descriptions.append(split_columns[2])

        channel_descriptions = set(channel_descriptions)

        for channel_description in channel_descriptions:
            channel_dictionary = perturbation.migration.models.find_channel_by(channels,
                                                                               channel_description)

            if not channel_dictionary:
                channel_dictionary = perturbation.migration.models.create_channel(channel_description,
                                                                                  channel_dictionary)

                channels.append(channel_dictionary)

        for column in columns:
            split_columns = column.split('_')

            a = None
            b = None
            if split_columns[0] == 'Correlation':
                for channel_dictionary in channels:
                    if channel_dictionary.description == split_columns[2]:
                        a = channel_dictionary

                    if channel_dictionary.description == split_columns[3]:
                        b = channel_dictionary

                correlation_columns.append((a, b))

        scales = []

        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'Texture':
                scales.append(split_columns[3])

        scales = set(scales)

        counts = []

        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'RadialDistribution':
                counts.append(split_columns[3].split('of')[0])

        counts = set(counts)

        moments = []

        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'AreaShape' and split_columns[1] == 'Zernike':
                moments.append((split_columns[2], split_columns[3]))

        for pattern in patterns:
            logger.debug('\tParse {}'.format(pattern.description))

            data = pandas.read_csv(os.path.join(directory, '{}.csv').format(pattern.description))

            with click.progressbar(length=data.shape[0], label="Processing " + pattern.description,
                                   show_eta=True) as bar:
                for index, row in data.iterrows():
                    bar.update(1)

                    row = collections.defaultdict(lambda: None, row)

                    image_id = perturbation.migration.models.find_image_by(
                            description='{}_{}'.format(
                                    digest,
                                    int(row[
                                            'ImageNumber'
                                        ])),
                            dictionaries=images
                    )

                    object_id = perturbation.migration.models.find_object_by(
                            description=str(int(
                                    row[
                                        'ObjectNumber'
                                    ]
                            )),
                            image_id=image_id,
                            dictionaries=objects
                    )

                    center = perturbation.migration.models.create_center(row)

                    coordinates.append(center)

                    neighborhood_dictionary = perturbation.migration.models.create_neighborhood(object_id, row)

                    if row['Neighbors_FirstClosestObjectNumber_5']:
                        description = str(int(row['Neighbors_FirstClosestObjectNumber_5']))

                        closest_id = perturbation.migration.models.find_object_by(
                                description=description,
                                image_id=image_id,
                                dictionaries=objects
                        )

                        neighborhood_dictionary._replace(closest_id=closest_id)

                    if row['Neighbors_SecondClosestObjectNumber_5']:
                        description = str(int(row['Neighbors_SecondClosestObjectNumber_5']))

                        second_closest_id = perturbation.migration.models.find_object_by(
                                description=description,
                                image_id=image_id,
                                dictionaries=objects
                        )

                        neighborhood_dictionary._replace(second_closest_id=second_closest_id)

                    neighborhoods.append(neighborhood_dictionary)

                    shape_center = perturbation.migration.models.create_shape_center(row)

                    coordinates.append(shape_center)

                    shape = perturbation.migration.models.create_shape(row, shape_center)

                    shapes.append(shape)

                    for a, b in moments:
                        moment_dictionary = perturbation.migration.models.create_moment(a, b, row, shape)

                        moments_group.append(moment_dictionary)

                    match = perturbation.migration.models.create_match(center, neighborhood_dictionary, object_id,
                                                                       pattern, shape)

                    matches.append(match)

                    for dependent, independent in correlation_columns:
                        correlation_dictionary = perturbation.migration.models.create_correlation(dependent,
                                                                                                  independent, match,
                                                                                                  row)

                        correlations.append(correlation_dictionary)

                    for channel_dictionary in channels:
                        intensity_dictionary = perturbation.migration.models.create_intensity(channel_dictionary, match,
                                                                                              row)

                        intensities.append(intensity_dictionary)

                        edges.append(
                            perturbation.migration.models.create_edge(channel_dictionary, match, row))

                        center_mass_intensity = perturbation.migration.models.create_center_mass_intensity(
                            channel_dictionary, row)

                        coordinates.append(center_mass_intensity)

                        max_intensity = perturbation.migration.models.create_max_intensity(channel_dictionary, row)

                        coordinates.append(max_intensity)

                        location = perturbation.migration.models.create_location(center_mass_intensity,
                                                                                 channel_dictionary, match,
                                                                                 max_intensity)

                        locations.append(location)

                        for scale in scales:
                            texture_dictionary = perturbation.migration.models.create_texture(channel_dictionary, match,
                                                                                              row, scale)

                            textures.append(texture_dictionary)

                        for count in counts:
                            radial_distribution_dictionary = perturbation.migration.models.create_radial_distribution(
                                channel_dictionary, count, match, row)

                            radial_distributions.append(radial_distribution_dictionary)

        perturbation.migration.models.save_coordinates(coordinates, session)

        perturbation.migration.models.save_correlations(correlation_offset, correlations, session)

        perturbation.migration.models.save_edges(edges, session)

        perturbation.migration.models.save_images(images, session)

        perturbation.migration.models.save_intensities(intensities, intensity_offset, session)

        perturbation.migration.models.save_locations(location_offset, locations, session)

        perturbation.migration.models.save_matches(matches, session)

        perturbation.migration.models.save_qualities(qualities, session)

        perturbation.migration.models.save_moments(moment_offset, moments, moments_group, session)

        perturbation.migration.models.save_neighborhoods(neighborhoods, session)

        perturbation.migration.models.save_objects(objects, session)

        perturbation.migration.models.save_radial_distributions(radial_distribution_offset, radial_distributions, session)

        perturbation.migration.models.save_shapes(session, shapes)

        perturbation.migration.models.save_textures(session, texture_offset, textures)

        perturbation.migration.models.save_wells(session, wells)

        logger.debug('\tCommit {}'.format(os.path.basename(directory)))

        session.commit()

    perturbation.migration.models.save_channels(channels, session)

    perturbation.migration.models.save_plates(plates, session)

    logger.debug('Commit plate, channel')

    session.commit()
