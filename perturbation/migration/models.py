import collections
import logging
import perturbation.models
import uuid

logger = logging.getLogger(__name__)

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


def create_channel(description, channel_dictionary):
    return __channel__(
        description=description,
        id=uuid.uuid4()
    )


def create_center(row):
    return __coordinate__(
        abscissa=row['Location_Center_X'],
        id=uuid.uuid4(),
        ordinate=row['Location_Center_Y']
    )

def create_center_mass_intensity(channel, row):
    return __coordinate__(
        abscissa=row['Location_CenterMassIntensity_X_{}'.format(channel.description)],
        id=uuid.uuid4(),
        ordinate=row['Location_CenterMassIntensity_Y_{}'.format(channel.description)]
    )


def create_correlation(dependent, independent, match, row):
    return __correlation__(
        coefficient=row['Correlation_Correlation_{}_{}'.format(dependent.description, independent.description)],
        dependent_id=dependent.id,
        id=None,
        independent_id=independent.id,
        match_id=match.id
    )


def create_edge(channel, match, row):
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


def create_max_intensity(channel, row):
    return __coordinate__(
        abscissa=row['Location_MaxIntensity_X_{}'.format(channel.description)],
        id=uuid.uuid4(),
        ordinate=row['Location_MaxIntensity_Y_{}'.format(channel.description)]
    )


def create_image(digest, description, well_dictionary):
    return __image__(
        description='{}_{}'.format(digest, int(description)),
        id=uuid.uuid4(),
        well_id=well_dictionary.id
    )


def create_intensity(channel, match, row):
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


def create_location(center_mass_intensity, channel, match, max_intensity):
    return __location__(
        center_mass_intensity_id=center_mass_intensity.id,
        channel_id=channel.id,
        id=None,
        match_id=match.id,
        max_intensity_id=max_intensity.id
    )


def create_match(center, neighborhood, object_id, pattern, shape):
    return __match__(
        center_id=center.id,
        id=uuid.uuid4(),
        neighborhood_id=neighborhood.id,
        object_id=object_id,
        pattern_id=pattern.id,
        shape_id=shape.id
    )


def create_moment(a, b, row, shape):
    return __moment__(
        a=a,
        b=b,
        id=None,
        score=row['AreaShape_Zernike_{}_{}'.format(a, b)],
        shape_id=shape.id
    )


def create_neighborhood(object_id, row):
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


def create_object(digest, images, description):
    return __object__(
        description=str(description['ObjectNumber']),
        id=uuid.uuid4(),
        image_id=find_image_by(description='{}_{}'.format(digest, int(description['ImageNumber'])), dictionaries=images)
    )


def create_plate(description, plate):
    return __plate__(
        description=str(int(description)),
        id=uuid.uuid4()
    )


def create_quality(data, image_description, image):
    return __quality__(
        id=uuid.uuid4(),
        image_id=image.id,
        count_cell_clump=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isCellClump']),
        count_debris=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isDebris']),
        count_low_intensity=int(data.loc[data['ImageNumber'] == image_description, 'Metadata_isLowIntensity'])
    )


def create_radial_distribution(channel, count, match, row):
    return __radial_distribution__(
        bins=count,
        channel_id=channel.id,
        frac_at_d=row['RadialDistribution_FracAtD_{}_{}of4'.format(channel.description, count)],
        id=None,
        match_id=match.id,
        mean_frac=row['RadialDistribution_MeanFrac_{}_{}of4'.format(channel.description, count)],
        radial_cv=row['RadialDistribution_RadialCV_{}_{}of4'.format(channel.description, count)]
    )


def create_shape(row, shape_center):
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


def create_shape_center(row):
    return __coordinate__(
        abscissa=row['AreaShape_Center_X'],
        id=uuid.uuid4(),
        ordinate=row['AreaShape_Center_Y']
    )


def create_texture(channel, match, row, scale):
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


def create_well(plate_dictionary, well_description):
    return __well__(
        description=well_description,
        id=uuid.uuid4(),
        plate_id=plate_dictionary.id
    )


def find_channel_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary.id


def find_image_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary.id


def find_object_by(description, dictionaries, image_id):
    for dictionary in dictionaries:
        if (dictionary.description == description) and (dictionary.image_id == image_id):
            return dictionary.id


def find_plate_by(dictionaries, description):
    for dictionary in dictionaries:
        if dictionary.description == description:
            return dictionary


def save_coordinates(coordinates, session):
    logger.debug('\tBulk insert Coordinate')

    __save__(perturbation.models.Coordinate, coordinates, session)

    coordinates.clear()


def save_correlations(offset, correlations, session):
    logger.debug('\tBulk insert Correlation')

    for index, correlation in enumerate(correlations):
        identifier = index + offset

        correlation._replace(id=identifier)

    offset += len(correlations)

    __save__(perturbation.models.Correlation, correlations, session)

    correlations.clear()


def save_edges(edges, session):
    logger.debug('\tBulk insert Edge')

    __save__(perturbation.models.Edge, edges, session)

    edges.clear()


def save_channels(channels, session):
    logger.debug('\tBulk insert Channel')

    __save__(perturbation.models.Channel, channels, session)

    channels.clear()


def save_plates(plates, session):
    logger.debug('\tBulk insert Plate')

    __save__(perturbation.models.Plate, plates, session)

    plates.clear()


def save_images(images, session):
    logger.debug('\tBulk insert Image')

    __save__(perturbation.models.Image, images, session)

    images.clear()


def save_intensities(intensities, offset, session):
    logger.debug('\tBulk insert Intensity')

    for index, intensity_dictionary in enumerate(intensities):
        intensity_dictionary._replace(id=index + offset)

    offset += len(intensities)

    __save__(perturbation.models.Intensity, intensities, session)

    intensities.clear()


def save_locations(offset, locations, session):
    logger.debug('\tBulk insert Location')

    for index, location_dictionary in enumerate(locations):
        location_dictionary._replace(id=index + offset)

    offset += len(locations)

    __save__(perturbation.models.Location, locations, session)

    locations.clear()


def save_matches(matches, session):
    logger.debug('\tBulk insert Match')

    __save__(perturbation.models.Match, matches, session)

    matches.clear()


def save_qualities(qualities, session):
    logger.debug('\tBulk insert Quality')

    __save__(perturbation.models.Quality, qualities, session)

    qualities.clear()


def save_wells(session, wells):
    logger.debug('\tBulk insert Well')

    __save__(perturbation.models.Well, wells, session)

    wells.clear()


def save_textures(session, offset, textures):
    logger.debug('\tBulk insert Texture')

    for index, texture_dictionary in enumerate(textures):
        texture_dictionary._replace(id=index + offset)

    offset += len(textures)

    __save__(perturbation.models.Texture, textures, session)

    textures.clear()


def save_objects(objects, session):
    logger.debug('\tBulk insert Object')

    __save__(perturbation.models.Object, objects, session)

    objects.clear()


def save_neighborhoods(neighborhoods, session):
    logger.debug('\tBulk insert Neighborhood')

    __save__(perturbation.models.Neighborhood, neighborhoods, session)

    neighborhoods.clear()


def save_moments(offset, moments, moments_group, session):
    logger.debug('\tBulk insert Moment')

    for index, moment_dictionary in enumerate(moments_group):
        moment_dictionary._replace(id=index + offset)

    offset += len(moments_group)

    __save__(perturbation.models.Moment, moments_group, session)

    moments.clear()


def save_shapes(session, shapes):
    logger.debug('\tBulk insert Shape')

    __save__(perturbation.models.Shape, shapes, session)

    shapes.clear()


def save_radial_distributions(offset, radial_distributions, session):
    logger.debug('\tBulk insert RadialDistribution')

    for index, radial_distribution_dictionary in enumerate(radial_distributions):
        radial_distribution_dictionary._replace(id=index + offset)

    offset += len(radial_distributions)

    __save__(perturbation.models.RadialDistribution, radial_distributions, session)

    radial_distributions.clear()


def __save__(table, records, session):
    def __create_mappings__(items):
        return [item._asdict() for item in items]

    session.bulk_insert_mappings(table, __create_mappings__(records))
