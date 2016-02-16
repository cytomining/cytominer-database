import click
import collections
import glob
import os
import pandas
import perturbation.base
from perturbation.models import *
from perturbation.sqlite3 import *
import sqlalchemy
import sqlite3
import sqlalchemy.orm
import logging

logger = logging.getLogger(__name__)

try:
    from line_profiler import LineProfiler


    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()

            return profiled_func

        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"

        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)

            return nothing

        return inner


def create(backend_file_path):
    connection = sqlite3.connect(backend_file_path)

    connection.create_aggregate('standard_deviation', 1, StandardDeviation)

    connection.create_function('standard_score', 3, standardize)

    return connection


#@do_profile(follow=[])
def seed(input, output, verbose=False):
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO if not verbose else logging.DEBUG)

    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)),
                                      creator=lambda: create(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

    images = []

    for chunk in pandas.read_csv(os.path.join(input, 'image.csv'), chunksize=4):
        for index, row in chunk.iterrows():
            well = Well.find_or_create_by(
                session=session,
                description=row['Metadata_Well']
            )

            plate = Plate.find_or_create_by(
                session=session,
                barcode=row['Metadata_Barcode']
            )

            plate.wells.append(well)

            image = Image.find_or_create_by(
                session=session,
                description=row['ImageNumber']
            )

            images.append(image)

            well.images.append(image)

    def find_image_by(image_description):
        for image in images:
            if image.description == image_description:
                return image

    objects = []

    def find_object_by(image_id, object_description):
        for object in objects:
            if str(object_description) == object_description:
                return object
            else:
                Object.find_or_create_by(session=session, description=object_description, image_id=image_id)


    # TODO: Read only the header, and read all the patterns because some columns are present in one and not the other
    data = pandas.read_csv(os.path.join(input, 'Cells.csv'))

    object_numbers = data[['ImageNumber', 'ObjectNumber']].drop_duplicates()

    for index, object_number in object_numbers.iterrows():
        object = Object(
            image=find_image_by(object_number['ImageNumber']),
            description=str(object_number['ObjectNumber'])
        )

        objects.append(object)

    session.add_all(objects)

    session.commit()

    filenames = []

    for filename in glob.glob(os.path.join(input, '*.csv')):
        if filename not in [os.path.join(input, 'image.csv'), os.path.join(input, 'object.csv')]:
            filenames.append(os.path.basename(filename))

    pattern_descriptions = []

    for filename in filenames:
        pattern_descriptions.append(filename.split('.')[0])

    patterns = []

    for pattern_description in pattern_descriptions:
        pattern = Pattern.find_or_create_by(session, description=pattern_description)

        patterns.append(pattern)


    columns = data.columns

    correlation_columns = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Correlation':
            a = Channel.find_or_create_by(session, description=split_columns[2])
            b = Channel.find_or_create_by(session, description=split_columns[3])

            correlation_columns.append((a, b))

    channel_descriptions = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Intensity':
            channel_descriptions.append(split_columns[2])

    channel_descriptions = set(channel_descriptions)

    channels = []

    for channel_description in channel_descriptions:
        channel = Channel.find_or_create_by(
            session,
            description=channel_description
        )

        channels.append(channel)

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

    session.commit()

    for pattern in patterns:
        data = pandas.read_csv(os.path.join(input, '{}.csv').format(pattern.description))

        with click.progressbar(length=data.shape[0], label="Processing " + pattern.description, show_eta=True) as bar:
            for index, row in data.iterrows():
                bar.update(1)

                row = collections.defaultdict(lambda: None, row)

                image = find_image_by(str(int(row['ImageNumber'])))

                object = find_object_by(
                    image_id=image.id,
                    object_description=str(int(row['ObjectNumber']))
                )

                match = Match()

                session.add(match)

                match.object = object

                center = Coordinate(
                    abscissa=int(round(row['Location_Center_X'])),
                    ordinate=int(round(row['Location_Center_Y']))
                )

                match.center = center

                shape = Shape(
                    area=row['AreaShape_Area'],
                    center=Coordinate(
                        abscissa=int(round(row['AreaShape_Center_X'])),
                        ordinate=int(round(row['AreaShape_Center_Y']))
                    ),
                    compactness=row['AreaShape_Compactness'],
                    eccentricity=row['AreaShape_Eccentricity'],
                    euler_number=row['AreaShape_EulerNumber'],
                    extent=row['AreaShape_Extent'],
                    form_factor=row['AreaShape_FormFactor'],
                    major_axis_length=row['AreaShape_MajorAxisLength'],
                    max_feret_diameter=row['AreaShape_MaxFeretDiameter'],
                    maximum_radius=row['AreaShape_MaximumRadius'],
                    mean_radius=row['AreaShape_MeanRadius'],
                    median_radius=row['AreaShape_MedianRadius'],
                    min_feret_diameter=row['AreaShape_MinFeretDiameter'],
                    minor_axis_length=row['AreaShape_MinorAxisLength'],
                    moments=[
                        Moment(
                            a=moment[0],
                            b=moment[1],
                            score=row[
                                'AreaShape_Zernike_{}_{}'.format(
                                    moment[0],
                                    moment[1]
                                )
                            ]
                        ) for moment in moments
                        ],
                    orientation=row['AreaShape_Orientation'],
                    perimeter=row['AreaShape_Perimeter'],
                    solidity=row['AreaShape_Solidity']
                )

                shape.center = center

                neighborhood = Neighborhood(
                    angle_between_neighbors_5=row['Neighbors_AngleBetweenNeighbors_5'],
                    angle_between_neighbors_adjacent=row['Neighbors_AngleBetweenNeighbors_Adjacent'],

                    first_closest_distance_5=row['Neighbors_FirstClosestDistance_5'],
                    first_closest_distance_adjacent=row['Neighbors_FirstClosestDistance_Adjacent'],
                    first_closest_object_number_adjacent=row['Neighbors_FirstClosestObjectNumber_Adjacent'],
                    number_of_neighbors_5=row['Neighbors_NumberOfNeighbors_5'],
                    number_of_neighbors_adjacent=row['Neighbors_NumberOfNeighbors_Adjacent'],
                    percent_touching_5=row['Neighbors_PercentTouching_5'],
                    percent_touching_adjacent=row['Neighbors_PercentTouching_Adjacent'],
                    second_closest_distance_5=row['Neighbors_SecondClosestDistance_5'],
                    second_closest_distance_adjacent=row['Neighbors_SecondClosestDistance_Adjacent'],
                    second_closest_object_number_adjacent=row['Neighbors_SecondClosestObjectNumber_Adjacent']
                )

                if row['Neighbors_FirstClosestObjectNumber_5']:
                    neighborhood.closest=find_object_by(
                        image_id=image.id,
                        object_description=str(int(row['Neighbors_FirstClosestObjectNumber_5']))
                    )

                if row['Neighbors_SecondClosestObjectNumber_5']:
                    neighborhood.second_closest=find_object_by(
                        image_id=image.id,
                        object_description=str(int(row['Neighbors_SecondClosestObjectNumber_5']))
                    )

                neighborhood.object = object

                match.neighborhood = neighborhood

                match.pattern = pattern

                match.shape = shape

                correlations = []

                for correlation_column in correlation_columns:
                    dependent, independent = correlation_column

                    correlation = Correlation(
                        coefficient=row[
                            'Correlation_Correlation_{}_{}'.format(
                                dependent.description,
                                independent.description
                            )
                        ],
                        dependent_id=dependent.id,
                        independent_id=independent.id,
                        match=match
                    )

                    correlations.append(correlation)

                session.add_all(correlations)

                intensities = []

                append_intensity = intensities.append

                edges = []

                append_edge = edges.append

                locations = []

                append_location = locations.append

                textures = []

                append_texture = textures.append

                radial_distributions = []

                append_radial_distribution = radial_distributions.append

                for channel in channels:
                    intensity = Intensity(
                        channel_id=channel.id,
                        first_quartile=row[
                            'Intensity_LowerQuartileIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        integrated=row[
                            'Intensity_IntegratedIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        mass_displacement=row[
                            'Intensity_MassDisplacement_{}'.format(
                                channel.description
                            )
                        ],
                        match=match,
                        maximum=row[
                            'Intensity_MaxIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        mean=row[
                            'Intensity_MeanIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        median=row[
                            'Intensity_MedianIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        median_absolute_deviation=row[
                            'Intensity_MADIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        minimum=row[
                            'Intensity_MinIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        standard_deviation=row[
                            'Intensity_StdIntensity_{}'.format(
                                channel.description
                            )
                        ],
                        third_quartile=row[
                            'Intensity_UpperQuartileIntensity_{}'.format(
                                channel.description
                            )
                        ]
                    )

                    append_intensity(intensity)

                    edge = Edge(
                        channel_id=channel.id,
                        integrated=row[
                            'Intensity_IntegratedIntensityEdge_{}'.format(
                                channel.description
                            )
                        ],
                        match=match,
                        maximum=row[
                            'Intensity_MaxIntensityEdge_{}'.format(
                                channel.description
                            )
                        ],
                        mean=row[
                            'Intensity_MeanIntensityEdge_{}'.format(
                                channel.description
                            )
                        ],
                        minimum=row[
                            'Intensity_MinIntensityEdge_{}'.format(
                                channel.description
                            )
                        ],
                        standard_deviation=row[
                            'Intensity_StdIntensityEdge_{}'.format(
                                channel.description
                            )
                        ]
                    )

                    append_edge(edge)

                    location = Location(
                        center_mass_intensity=Coordinate(
                            abscissa=int(
                                round(
                                    row[
                                        'Location_CenterMassIntensity_X_{}'.format(
                                            channel.description
                                        )
                                    ]
                                )
                            ),
                            ordinate=int(
                                round(
                                    row[
                                        'Location_CenterMassIntensity_Y_{}'.format(
                                            channel.description
                                        )
                                    ]
                                )
                            )
                        ),
                        channel_id=channel.id,
                        match=match,
                        max_intensity=Coordinate(
                            abscissa=int(
                                round(
                                    row[
                                        'Location_MaxIntensity_X_{}'.format(
                                            channel.description
                                        )
                                    ]
                                )
                            ),
                            ordinate=int(
                                round(
                                    row[
                                        'Location_MaxIntensity_Y_{}'.format(
                                            channel.description
                                        )
                                    ]
                                )
                            )
                        )
                    )

                    append_location(location)

                    for scale in scales:
                        texture = Texture(
                            angular_second_moment=row[
                                'Texture_AngularSecondMoment_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            channel_id=channel.id,
                            contrast=row[
                                'Texture_Contrast_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            correlation=row[
                                'Texture_Correlation_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            difference_entropy=row[
                                'Texture_DifferenceEntropy_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            difference_variance=row[
                                'Texture_DifferenceVariance_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            match=match,
                            scale=scale,
                            entropy=row[
                                'Texture_Entropy_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            gabor=row[
                                'Texture_Gabor_{}_{}'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            info_meas_1=row[
                                'Texture_InfoMeas1_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            info_meas_2=row[
                                'Texture_InfoMeas2_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            inverse_difference_moment=row[
                                'Texture_InverseDifferenceMoment_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            sum_average=row[
                                'Texture_SumAverage_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            sum_entropy=row[
                                'Texture_SumEntropy_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            sum_variance=row[
                                'Texture_SumVariance_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
                            variance=row[
                                'Texture_Variance_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ]
                        )

                        append_texture(texture)

                    for count in counts:
                        radial_distribution = RadialDistribution(
                            bins=count,
                            channel_id=channel.id,
                            frac_at_d=row[
                                'RadialDistribution_FracAtD_{}_{}of4'.format(
                                    channel.description,
                                    count
                                )
                            ],
                            match=match,
                            mean_frac=row[
                                'RadialDistribution_MeanFrac_{}_{}of4'.format(
                                    channel.description,
                                    count
                                )
                            ],
                            radial_cv=row[
                                'RadialDistribution_RadialCV_{}_{}of4'.format(
                                    channel.description,
                                    count
                                )
                            ]
                        )

                        append_radial_distribution(radial_distribution)

                session.add_all(intensities)
                session.add_all(edges)
                session.add_all(locations)
                session.add_all(textures)
                session.add_all(radial_distributions)

        session.commit()
