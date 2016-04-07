import click
import glob
import hashlib
import os
import pandas
import perturbation.base
import perturbation.migration
import perturbation.models
import sqlalchemy
import sqlalchemy.orm
import collections
import logging
import perturbation.models
import sqlalchemy.exc
import sqlalchemy.ext.declarative
import perturbation.UUID
import uuid
import sqlalchemy.types


class UUID(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.types.BINARY(16)

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                try:
                    value = uuid.UUID(value)
                except(TypeError, ValueError):
                    value = uuid.UUID(bytes=value)

            return value.bytes

    def process_literal_param(self, value, dialect):
        pass

    def process_result_value(self, value, dialect):
        pass

    def python_type(self):
        pass

@sqlalchemy.ext.declarative.as_declarative()
class Base(object):
    id = sqlalchemy.Column(UUID, default=uuid.uuid4, primary_key=True)

    @classmethod
    def find_or_create_by(cls, session, create_method='', create_method_kwargs=None, **kwargs):
        try:
            return session.query(cls).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            kwargs.update(create_method_kwargs or {})

            created = getattr(cls, create_method, cls)(**kwargs)

            try:
                session.add(created)

                session.flush()

                return created
            except sqlalchemy.exc.IntegrityError:
                session.rollback()

                return session.query(cls).filter_by(**kwargs).one()

logger = logging.getLogger(__name__)

Base = perturbation.base.Base

Session = sqlalchemy.orm.sessionmaker()

engine = None

scoped_session = sqlalchemy.orm.scoped_session(Session)

cdef int correlation_offset = 0
cdef int intensity_offset = 0
cdef int location_offset = 0
cdef int moment_offset = 0
cdef int texture_offset = 0
cdef int radial_distribution_offset = 0

cdef list channels = []
cdef list coordinates = []
cdef list correlations = []
cdef list edges = []
cdef list images = []
cdef list intensities = []
cdef list locations = []
cdef list qualities = []
cdef list matches = []
cdef list neighborhoods = []
cdef list plates = []
cdef list radial_distributions = []
cdef list shapes = []
cdef list textures = []
cdef list wells = []

__channel__ = collections.namedtuple(
        field_names=[
            'description',
            'id'
        ],
        typename='Channel'
)

__coordinate__ = collections.namedtuple(
        field_names=[
            'abscissa',
            'id',
            'ordinate'
        ],
        typename='Coordinate'
)

__correlation__ = collections.namedtuple(
        field_names=[
            'coefficient',
            'dependent_id',
            'id',
            'independent_id',
            'match_id'
        ],
        typename='Correlation'
)

__edge__ = collections.namedtuple(
        typename='Edge',
        field_names=[
            'channel_id',
            'id',
            'integrated',
            'match_id',
            'maximum',
            'mean',
            'minimum',
            'standard_deviation'
        ]
)

__image__ = collections.namedtuple(
        field_names=[
            'description',
            'id',
            'well_id'
        ],
        typename='Image'
)

__intensity__ = collections.namedtuple(
        field_names=[
            'channel_id',
            'first_quartile',
            'id',
            'integrated',
            'mass_displacement',
            'match_id',
            'maximum',
            'mean',
            'median',
            'median_absolute_deviation',
            'minimum',
            'standard_deviation',
            'third_quartile'
        ],
        typename='Intensity'
)

__location__ = collections.namedtuple(
        field_names=[
            'center_mass_intensity_id',
            'channel_id',
            'id',
            'match_id',
            'max_intensity_id'
        ],
        typename='Location'
)

__match__ = collections.namedtuple(
        field_names=[
            'center_id',
            'id',
            'neighborhood_id',
            'object_id',
            'pattern_id',
            'shape_id'
        ],
        typename='Match'
)

__moment__ = collections.namedtuple(
        field_names=[
            'a',
            'b',
            'id',
            'score',
            'shape_id'
        ],
        typename='Moment'
)

__object__ = collections.namedtuple(
        field_names=[
            'description',
            'id',
            'image_id'
        ],
        typename='Object'
)

__pattern__ = collections.namedtuple(
        field_names=[
            'description',
            'id'
        ],
        typename='Pattern'
)

__plate__ = collections.namedtuple(
        field_names=[
            'description',
            'id'
        ],
        typename='Plate'
)

__quality__ = collections.namedtuple(
        field_names=[
            'count_cell_clump',
            'count_debris',
            'count_low_intensity',
            'id',
            'image_id'
        ],
        typename='Quality'
)

__radial_distribution__ = collections.namedtuple(
        field_names=[
            'bins',
            'channel_id',
            'frac_at_d',
            'id',
            'match_id',
            'mean_frac',
            'radial_cv'
        ],
        typename='RadialDistribution'
)

__neighborhood__ = collections.namedtuple(
        field_names=[
            'angle_between_neighbors_5',
            'angle_between_neighbors_adjacent',
            'closest_id',
            'first_closest_distance_5',
            'first_closest_distance_adjacent',
            'first_closest_object_number_adjacent',
            'id',
            'number_of_neighbors_5',
            'number_of_neighbors_adjacent',
            'object_id',
            'percent_touching_5',
            'percent_touching_adjacent',
            'second_closest_distance_5',
            'second_closest_distance_adjacent',
            'second_closest_id',
            'second_closest_object_number_adjacent'
        ],
        typename='Neighborhood'
)

__shape__ = collections.namedtuple(
        typename='Shape',
        field_names=[
            'area',
            'center_id',
            'compactness',
            'eccentricity',
            'euler_number',
            'extent',
            'form_factor',
            'id',
            'major_axis_length',
            'max_feret_diameter',
            'maximum_radius',
            'mean_radius',
            'median_radius',
            'min_feret_diameter',
            'minor_axis_length',
            'orientation',
            'perimeter',
            'solidity'
        ]
)

__texture__ = collections.namedtuple(
        typename='Texture',
        field_names=[
            'angular_second_moment',
            'channel_id',
            'contrast',
            'correlation',
            'difference_entropy',
            'difference_variance',
            'match_id',
            'scale',
            'entropy',
            'gabor',
            'id',
            'info_meas_1',
            'info_meas_2',
            'inverse_difference_moment',
            'sum_average',
            'sum_entropy',
            'sum_variance',
            'variance'
        ]
)

__well__ = collections.namedtuple(
        field_names=[
            'description',
            'id',
            'plate_id'
        ],
        typename='Well'
)

cdef setup(database):
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
    for directory in find_directories(directories):
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        logger.debug('Parsing {}'.format(os.path.basename(directory)))

        moments_group = []

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        plate_descriptions = data['Metadata_Barcode'].unique()

        logger.debug('\tParse plates, wells, images, quality')

        create_plates(data, digest, images, plate_descriptions, plates, qualities, wells)

        logger.debug('\tParse objects')

        # TODO: Read all the patterns because some columns are present in only one pattern
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        object_numbers = pandas.concat([get_object_numbers(s) for s in ['ObjectNumber', 'Neighbors_FirstClosestObjectNumber_5', 'Neighbors_SecondClosestObjectNumber_5']])

        object_numbers.drop_duplicates()

        objects = find_objects(digest, images, object_numbers)

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

    save_channels(channels)

    save_plates(plates)

    logger.debug('Commit plate, channel')


cdef create_patterns(channels, coordinates, correlation_columns, correlation_offset, correlations, counts, digest, directory, edges, images, intensities, intensity_offset, location_offset, locations, matches, moment_offset, moments, moments_group, neighborhoods, objects, patterns, qualities, radial_distribution_offset, radial_distributions, scales, shapes, texture_offset, textures, wells):
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


cdef find_pattern_descriptions(filenames):
    pattern_descriptions = []

    for filename in filenames:
        pattern_descriptions.append(filename.split('.')[0])

    return pattern_descriptions


cdef find_objects(digest, images, object_numbers):
    objects = []

    for index, object_number in object_numbers.iterrows():
        object_dictionary = create_object(digest, images, object_number)

        objects.append(object_dictionary)

    return objects


cdef find_patterns(pattern_descriptions, session):
    patterns = []

    for pattern_description in pattern_descriptions:
        pattern = perturbation.models.Pattern.find_or_create_by(session=session, description=pattern_description)

        patterns.append(pattern)

    return patterns


cdef find_channel_descriptions(channels, columns):
    channel_descriptions = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Intensity':
            channel_descriptions.append(split_columns[2])

    channel_descriptions = set(channel_descriptions)

    for channel_description in channel_descriptions:
        channel = find_channel_by(channels, channel_description)

        if not channel:
            channel = create_channel(channel_description, channel)

            channels.append(channel)


cdef find_moments(columns):
    moments = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'AreaShape' and split_columns[1] == 'Zernike':
            moments.append((split_columns[2], split_columns[3]))

    return moments


cdef find_counts(columns):
    counts = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'RadialDistribution':
            counts.append(split_columns[3].split('of')[0])

    counts = set(counts)

    return counts


cdef find_scales(columns):
    scales = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Texture':
            scales.append(split_columns[3])

    scales = set(scales)

    return scales


cdef find_correlation_columns(channels, columns):
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


cdef create_correlations(correlation_columns, correlations, match, row):
    for dependent, independent in correlation_columns:
        correlation = create_correlation(dependent, independent, match, row)

        correlations.append(correlation)


cdef create_views(sqlfile):
    logger.debug('Parsing SQL file')

    with open(sqlfile) as f:
        import sqlparse

        for s in sqlparse.split(f.read()):
            engine.execute(s)


cdef create_moments(moments, moments_group, row, shape):
    for a, b in moments:
        moment = create_moment(a, b, row, shape)

        moments_group.append(moment)


cdef create_channels(channels, coordinates, counts, edges, intensities, locations, match, radial_distributions, row, scales, textures):
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


cdef create_radial_distributions(channel, counts, match, radial_distributions, row):
    for count in counts:
        radial_distribution = create_radial_distribution(channel, count, match, row)

        radial_distributions.append(radial_distribution)


def create_textures(channel, match, row, scales, textures):
    for scale in scales:
        texture = create_texture(channel, match, row, scale)

        textures.append(texture)


cdef create_images(data, digest, descriptions, images, qualities, well):
    for description in descriptions:
        image = create_image(digest, description, well)

        images.append(image)

        quality = create_quality(data, description, image)

        qualities.append(quality)


cdef create_plates(data, digest, images, descriptions, plates, qualities, wells):
    for description in descriptions:
        plate = find_plate_by(plates, str(int(description)))

        if not plate:
            plate = create_plate(description, plate)

            plates.append(plate)

        well_descriptions = data[data['Metadata_Barcode'] == description]['Metadata_Well'].unique()

        create_wells(data, digest, images, plate, description, qualities, well_descriptions, wells)


cdef create_wells(data, digest, images, plate, plate_description, qualities, descriptions, wells):
    for description in descriptions:
        well = create_well(plate, description)

        wells.append(well)

        image_descriptions = data[(data['Metadata_Barcode'] == plate_description) & (data['Metadata_Well'] == description)]['ImageNumber'].unique()

        create_images(data, digest, image_descriptions, images, qualities, well)

cdef find_directories(directory):
    directories = []

    filenames = glob.glob(os.path.join(directory, '*'))

    for filename in filenames:
        directories.append(os.path.relpath(filename))

    return set(directories)


cdef create_channel(description, channel_dictionary):
    return __channel__(
            description=description,
            id=uuid.uuid4()
    )


cdef create_center(row):
    return __coordinate__(
            abscissa=row['Location_Center_X'],
            id=uuid.uuid4(),
            ordinate=row['Location_Center_Y']
    )


cdef create_center_mass_intensity(channel, row):
    return __coordinate__(
            abscissa=row['Location_CenterMassIntensity_X_{}'.format(channel.description)],
            id=uuid.uuid4(),
            ordinate=row['Location_CenterMassIntensity_Y_{}'.format(channel.description)]
    )


cdef create_correlation(dependent, independent, match, row):
    return __correlation__(
            coefficient=row['Correlation_Correlation_{}_{}'.format(dependent.description, independent.description)],
            dependent_id=dependent.id,
            id=None,
            independent_id=independent.id,
            match_id=match.id
    )


cdef create_edge(channel, match, row):
    return __edge__(
            channel_id=channel.id,
            id=uuid.uuid4(),
            integrated=row['Intensity_IntegratedIntensityEdge_{}'.format(channel.description)],
            match_id=match.id,
            maximum=row['Intensity_MaxIntensityEdge_{}'.format(channel.description)],
            mean=row['Intensity_MeanIntensityEdge_{}'.format(channel.description)],
            minimum=row['Intensity_MinIntensityEdge_{}'.format(channel.description)],
            standard_deviation=row['Intensity_StdIntensityEdge_{}'.format(channel.description)]
    )


cdef create_max_intensity(channel, row):
    return __coordinate__(
            abscissa=row['Location_MaxIntensity_X_{}'.format(channel.description)],
            id=uuid.uuid4(),
            ordinate=row['Location_MaxIntensity_Y_{}'.format(channel.description)]
    )


cdef create_image(digest, description, well_dictionary):
    return __image__(
            description='{}_{}'.format(digest, int(description)),
            id=uuid.uuid4(),
            well_id=well_dictionary.id
    )


cdef create_intensity(channel, match, row):
    return __intensity__(
            channel_id=channel.id,
            first_quartile=row['Intensity_LowerQuartileIntensity_{}'.format(channel.description)],
            id=None,
            integrated=row['Intensity_IntegratedIntensity_{}'.format(channel.description)],
            mass_displacement=row['Intensity_MassDisplacement_{}'.format(channel.description)],
            match_id=match.id,
            maximum=row['Intensity_MaxIntensity_{}'.format(channel.description)],
            mean=row['Intensity_MeanIntensity_{}'.format(channel.description)],
            median=row['Intensity_MedianIntensity_{}'.format(channel.description)],
            median_absolute_deviation=row['Intensity_MADIntensity_{}'.format(channel.description)],
            minimum=row['Intensity_MinIntensity_{}'.format(channel.description)],
            standard_deviation=row['Intensity_StdIntensity_{}'.format(channel.description)],
            third_quartile=row['Intensity_UpperQuartileIntensity_{}'.format(channel.description)]
    )


cdef create_location(center_mass_intensity, channel, match, max_intensity):
    return __location__(
            center_mass_intensity_id=center_mass_intensity.id,
            channel_id=channel.id,
            id=None,
            match_id=match.id,
            max_intensity_id=max_intensity.id
    )


cdef create_match(center, neighborhood, object_id, pattern, shape):
    return __match__(
            center_id=center.id,
            id=uuid.uuid4(),
            neighborhood_id=neighborhood.id,
            object_id=object_id,
            pattern_id=pattern.id,
            shape_id=shape.id
    )


cdef create_moment(a, b, row, shape):
    return __moment__(
            a=a,
            b=b,
            id=None,
            score=row['AreaShape_Zernike_{}_{}'.format(a, b)],
            shape_id=shape.id
    )


cdef create_neighborhood(object_id, row):
    return __neighborhood__(
            angle_between_neighbors_5=row['Neighbors_AngleBetweenNeighbors_5'],
            angle_between_neighbors_adjacent=row['Neighbors_AngleBetweenNeighbors_Adjacent'],
            closest_id=None,
            first_closest_distance_5=row['Neighbors_FirstClosestDistance_5'],
            first_closest_distance_adjacent=row['Neighbors_FirstClosestDistance_Adjacent'],
            first_closest_object_number_adjacent=row['Neighbors_FirstClosestObjectNumber_Adjacent'],
            id=uuid.uuid4(),
            number_of_neighbors_5=row['Neighbors_NumberOfNeighbors_5'],
            number_of_neighbors_adjacent=row['Neighbors_NumberOfNeighbors_Adjacent'],
            object_id=object_id,
            percent_touching_5=row['Neighbors_PercentTouching_5'],
            percent_touching_adjacent=row['Neighbors_PercentTouching_Adjacent'],
            second_closest_distance_5=row['Neighbors_SecondClosestDistance_5'],
            second_closest_distance_adjacent=row['Neighbors_SecondClosestDistance_Adjacent'],
            second_closest_id=None,
            second_closest_object_number_adjacent=row['Neighbors_SecondClosestObjectNumber_Adjacent']
    )


cdef create_object(digest, images, description):
    return __object__(
            description=str(description['ObjectNumber']),
            id=uuid.uuid4(),
            image_id=find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])), dictionaries=images)
    )


cdef create_plate(description, plate):
    return __plate__(
            description=str(int(description)),
            id=uuid.uuid4()
    )


cdef create_quality(data, image_description, image):
    return __quality__(
            id=uuid.uuid4(),
            image_id=image.id,
            count_cell_clump=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isCellClump']),
            count_debris=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isDebris']),
            count_low_intensity=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isLowIntensity'])
    )


cdef create_radial_distribution(channel, count, match, row):
    return __radial_distribution__(
            bins=count,
            channel_id=channel.id,
            frac_at_d=row['RadialDistribution_FracAtD_{}_{}of4'.format(channel.description, count)],
            id=None,
            match_id=match.id,
            mean_frac=row['RadialDistribution_MeanFrac_{}_{}of4'.format(channel.description, count)],
            radial_cv=row['RadialDistribution_RadialCV_{}_{}of4'.format(channel.description, count)]
    )


cdef create_shape(row, shape_center):
    return __shape__(
            area=row['AreaShape_Area'],
            center_id=shape_center.id,
            compactness=row['AreaShape_Compactness'],
            eccentricity=row['AreaShape_Eccentricity'],
            euler_number=row['AreaShape_EulerNumber'],
            extent=row['AreaShape_Extent'],
            form_factor=row['AreaShape_FormFactor'],
            id=uuid.uuid4(),
            major_axis_length=row['AreaShape_MajorAxisLength'],
            max_feret_diameter=row['AreaShape_MaxFeretDiameter'],
            maximum_radius=row['AreaShape_MaximumRadius'],
            mean_radius=row['AreaShape_MeanRadius'],
            median_radius=row['AreaShape_MedianRadius'],
            min_feret_diameter=row['AreaShape_MinFeretDiameter'],
            minor_axis_length=row['AreaShape_MinorAxisLength'],
            orientation=row['AreaShape_Orientation'],
            perimeter=row['AreaShape_Perimeter'],
            solidity=row['AreaShape_Solidity']
    )


cdef create_shape_center(row):
    return __coordinate__(
            abscissa=row['AreaShape_Center_X'],
            id=uuid.uuid4(),
            ordinate=row['AreaShape_Center_Y']
    )


cdef create_texture(channel, match, row, scale):
    def find_by(key):
        return row[
            'Texture_{}_{}_{}_0'.format(
                    key,
                    channel.description,
                    scale
            )
        ]

    return __texture__(
            angular_second_moment=find_by('AngularSecondMoment'),
            channel_id=channel.id,
            contrast=find_by('Contrast'),
            correlation=find_by('Correlation'),
            difference_entropy=find_by('DifferenceEntropy'),
            difference_variance=find_by('DifferenceVariance'),
            match_id=match.id,
            scale=scale,
            entropy=find_by('Entropy'),
            gabor=find_by('Gabor'),
            id=None,
            info_meas_1=find_by('InfoMeas1'),
            info_meas_2=find_by('InfoMeas2'),
            inverse_difference_moment=find_by('InverseDifferenceMoment'),
            sum_average=find_by('SumAverage'),
            sum_entropy=find_by('SumEntropy'),
            sum_variance=find_by('SumVariance'),
            variance=find_by('Variance')
    )


cdef create_well(plate_dictionary, well_description):
    return __well__(
            description=well_description,
            id=uuid.uuid4(),
            plate_id=plate_dictionary.id
    )


cdef find_channel_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary.id


cdef find_image_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary.id


cdef find_object_by(description, dictionaries, image_id):
    for dictionary in dictionaries:
        if (dictionary.description == description) and (dictionary.image_id == image_id):
            return dictionary.id


cdef find_plate_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary


cdef save_coordinates(coordinates):
    logger.debug('\tBulk insert Coordinate')

    __save__(perturbation.models.Coordinate, coordinates)


cdef save_correlations(offset, correlations):
    logger.debug('\tBulk insert Correlation')

    __save__(perturbation.models.Correlation, correlations, offset)


cdef save_edges(edges):
    logger.debug('\tBulk insert Edge')

    __save__(perturbation.models.Edge, edges)


cdef save_channels(channels):
    logger.debug('\tBulk insert Channel')

    __save__(perturbation.models.Channel, channels)


cdef save_plates(plates):
    logger.debug('\tBulk insert Plate')

    __save__(perturbation.models.Plate, plates)


cdef save_images(images):
    logger.debug('\tBulk insert Image')

    __save__(perturbation.models.Image, images)


cdef save_intensities(intensities, offset):
    logger.debug('\tBulk insert Intensity')

    __save__(perturbation.models.Intensity, intensities, offset)


cdef save_locations(offset, locations):
    logger.debug('\tBulk insert Location')

    __save__(perturbation.models.Location, locations, offset)


cdef save_matches(matches):
    logger.debug('\tBulk insert Match')

    __save__(perturbation.models.Match, matches)


cdef save_qualities(qualities):
    logger.debug('\tBulk insert Quality')

    __save__(perturbation.models.Quality, qualities)


cdef save_wells(wells):
    logger.debug('\tBulk insert Well')

    __save__(perturbation.models.Well, wells)


cdef save_textures(offset, textures):
    logger.debug('\tBulk insert Texture')

    __save__(perturbation.models.Texture, textures, offset)


cdef save_objects(objects):
    logger.debug('\tBulk insert Object')

    __save__(perturbation.models.Object, objects)


cdef save_neighborhoods(neighborhoods):
    logger.debug('\tBulk insert Neighborhood')

    __save__(perturbation.models.Neighborhood, neighborhoods)


cdef save_moments(offset, moments, moments_group):
    logger.debug('\tBulk insert Moment')

    __save__(perturbation.models.Moment, moments_group, offset)


cdef save_shapes(shapes):
    logger.debug('\tBulk insert Shape')

    __save__(perturbation.models.Shape, shapes)


cdef save_radial_distributions(offset, radial_distributions):
    logger.debug('\tBulk insert RadialDistribution')

    __save__(perturbation.models.RadialDistribution, radial_distributions, offset)


def __save__(table, records, offset=None):
    def __create_mappings__(items):
        return [item._asdict() for item in items]

    if offset:
        for index, record in enumerate(records):
            record._replace(id=index + offset)

            offset += len(records)

    scoped_session.bulk_insert_mappings(table, __create_mappings__(records))

    scoped_session.commit()

    records.clear()
