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

correlation_offset = 0
intensity_offset = 0
location_offset = 0
moment_offset = 0
texture_offset = 0
radial_distribution_offset = 0

channels = []
coordinates = []
correlations = []
edges = []
images = []
intensities = []
locations = []
qualities = []
matches = []
neighborhoods = []
plates = []
radial_distributions = []
shapes = []
textures = []
wells = []


def setup(database):
    global engine

    engine = sqlalchemy.create_engine("sqlite:///{}".format(os.path.realpath(database)))

    scoped_session.remove()

    scoped_session.configure(autoflush=False, bind=engine, expire_on_commit=False)

    Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)


def seed(input, output, sqlfile, verbose=False):
    setup(output)

    create_views(sqlfile)

    seed_plate(input)


def seed_plate(directories):
    pathnames = find_directories(directories)

    for directory in pathnames:
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        moments_group = []

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        #TODO: The string 'Metadata_Barcode' should be gotten from a config file
        plate_descriptions = data['Metadata_Barcode'].unique()

        create_plates(data, digest, images, plate_descriptions, plates, qualities, wells)

        # TODO: Read all the patterns because some columns are present in only one pattern
        # TODO: This assumes that Cells.csv exists, whereas it could be any pattern
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        # TODO: Avoid needing to explicity names all *ObjectNumber* columns
        object_numbers = pandas.concat([get_object_numbers(s) for s in ['ObjectNumber', 'Neighbors_FirstClosestObjectNumber_5', 'Neighbors_SecondClosestObjectNumber_5']])

        object_numbers.drop_duplicates()

        objects = find_objects(digest, images, object_numbers)

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

    save_channels(channels)

    save_plates(plates)


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

                create_moments(moments, moments_group, row, shape)

                match = create_match(center, neighborhood, object_id, pattern, shape)

                matches.append(match)

                create_correlations(correlation_columns, correlations, match, row)

                create_channels(channels, coordinates, counts, edges, intensities, locations, match, radial_distributions, row, scales, textures)

    save_coordinates(coordinates)
    save_edges(edges)
    save_images(images)
    save_matches(matches)
    save_neighborhoods(neighborhoods)
    save_objects(objects)
    save_qualities(qualities)
    save_shapes(shapes)
    save_textures(texture_offset, textures)
    save_wells(wells)
    save_correlations(correlation_offset, correlations)
    save_intensities(intensities, intensity_offset)
    save_locations(location_offset, locations)
    save_moments(moment_offset, moments, moments_group)
    save_radial_distributions(radial_distribution_offset, radial_distributions)

    logger.debug('\tCommit {}'.format(os.path.basename(directory)))


def find_channel_descriptions(channels, columns):
    channel_descriptions = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Intensity":
            channel_description = split_columns[2]

            channel_descriptions.add(channel_description)

    for channel_description in channel_descriptions:
        channel = find_channel_by(channels, channel_description)

        if not channel:
            channel = create_channel(channel_description, channel)

            channels.append(channel)


def find_correlation_columns(channels, columns):
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


def find_objects(digest, images, object_numbers):
    objects = []

    for index, object_number in object_numbers.iterrows():
        object_dictionary = create_object(digest, images, object_number)

        objects.append(object_dictionary)

    return objects


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


def create_correlations(correlation_columns, correlations, match, row):
    for dependent, independent in correlation_columns:
        correlation = create_correlation(dependent, independent, match, row)

        correlations.append(correlation)


def create_images(data, digest, descriptions, images, qualities, well):
    for description in descriptions:
        image = create_image(digest, description, well)

        images.append(image)

        quality = create_quality(data, description, image)

        qualities.append(quality)


def create_moments(moments, moments_group, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


def create_plates(data, digest, images, descriptions, plates, qualities, wells):
    for description in descriptions:
        plate = find_plate_by(plates, str(int(description)))

        if not plate:
            plate = create_plate(description, plate)

            plates.append(plate)

        well_descriptions = data[data['Metadata_Barcode'] == description]['Metadata_Well'].unique()

        create_wells(data, digest, images, plate, description, qualities, well_descriptions, wells)


def create_radial_distributions(channel, counts, match, radial_distributions, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales, textures):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

        textures.append(texture)


def create_views(sqlfile):
    logger.debug('Parsing SQL file')

    with open(sqlfile) as f:
        import sqlparse

        for s in sqlparse.split(f.read()):
            engine.execute(s)


def create_wells(data, digest, images, plate, plate_description, qualities, descriptions, wells):
    for description in descriptions:
        well = create_well(plate, description)

        wells.append(well)

        image_descriptions = data[(data['Metadata_Barcode'] == plate_description) & (data['Metadata_Well'] == description)]['ImageNumber'].unique()

        create_images(data, digest, image_descriptions, images, qualities, well)


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


def create_channel(description, channel_dictionary):
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
            "id": None,
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
            "id": None,
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
            "id": None,
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
            "id": None,
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


def create_object(digest, images, description):
    return {
            "description": str(description['ObjectNumber']),
            "id": uuid.uuid4(),
            "image_id": find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])), dictionaries=images)
    }


def create_plate(description, plate):
    return {
            "description": str(int(description)),
            "id": uuid.uuid4()
    }


def create_quality(data, image_description, image):
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
            "id": None,
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
            "id": None,
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

def save_coordinates(coordinates):
    __save__(perturbation.models.Coordinate, coordinates)


def save_correlations(offset, correlations):
    __save__(perturbation.models.Correlation, correlations, offset)


def save_edges(edges):
    __save__(perturbation.models.Edge, edges)


def save_channels(channels):
    __save__(perturbation.models.Channel, channels)


def save_plates(plates):
    __save__(perturbation.models.Plate, plates)


def save_images(images):
    __save__(perturbation.models.Image, images)


def save_intensities(intensities, offset):
    __save__(perturbation.models.Intensity, intensities, offset)


def save_locations(offset, locations):
    __save__(perturbation.models.Location, locations, offset)


def save_matches(matches):
    __save__(perturbation.models.Match, matches)


def save_qualities(qualities):
    __save__(perturbation.models.Quality, qualities)


def save_wells(wells):
    __save__(perturbation.models.Well, wells)


def save_textures(offset, textures):
    __save__(perturbation.models.Texture, textures, offset)


def save_objects(objects):
    __save__(perturbation.models.Object, objects)


def save_neighborhoods(neighborhoods):
    __save__(perturbation.models.Neighborhood, neighborhoods)


def save_moments(offset, moments, moments_group):
    __save__(perturbation.models.Moment, moments_group, offset)


def save_shapes(shapes):
    __save__(perturbation.models.Shape, shapes)


def save_radial_distributions(offset, radial_distributions):
    __save__(perturbation.models.RadialDistribution, radial_distributions, offset)


def __save__(table, records, offset=None):
    if offset:
        for index, record in enumerate(records):
            record.update(id=index + offset)

            offset += len(records)

    scoped_session.execute(table.__table__.insert(), records)

    scoped_session.commit()

    records.clear()
