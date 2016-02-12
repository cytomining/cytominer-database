import click
import glob
import IPython
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


# @do_profile(follow=[])
def seed(input, output, verbose=False):
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO if not verbose else logging.DEBUG)

    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)), creator=lambda: create(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

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

            well.images.append(image)

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

    data = pandas.read_csv(os.path.join(input, 'Cells.csv'))

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

    if verbose:
        logger.debug("Debug mode: only read csvs but don't populate db.")

    for pattern in patterns:
        data = pandas.read_csv(os.path.join(input, '{}.csv').format(pattern.description))

        with click.progressbar(length=data.shape[0], label="Processing " + pattern.description, show_eta=True) as bar:
            for index, row in data.iterrows():
                bar.update(1)

                image = Image.find_by(
                    session=session,
                    description=int(row['ImageNumber'])
                )

                object = Object.find_or_create_by(
                    description=row['ObjectNumber'],
                    image_id = image.id,
                    session=session
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

                try:
                    neighborhood = Neighborhood(
                        angle_between_neighbors_5=row['Neighbors_AngleBetweenNeighbors_5'],
                        angle_between_neighbors_adjacent=row['Neighbors_AngleBetweenNeighbors_Adjacent'],
                        closest=Object.find_or_create_by(
                            session=session,
                            image=Image.find_by(
                                session=session,
                                description=int(row['ImageNumber'])
                            ),
                            description=row['Neighbors_FirstClosestObjectNumber_5']
                        ),
                        first_closest_distance_5=row['Neighbors_FirstClosestDistance_5'],
                        first_closest_distance_adjacent=row['Neighbors_FirstClosestDistance_Adjacent'],
                        first_closest_object_number_adjacent=row['Neighbors_FirstClosestObjectNumber_Adjacent'],
                        number_of_neighbors_5=row['Neighbors_NumberOfNeighbors_5'],
                        number_of_neighbors_adjacent=row['Neighbors_NumberOfNeighbors_Adjacent'],
                        percent_touching_5=row['Neighbors_PercentTouching_5'],
                        percent_touching_adjacent=row['Neighbors_PercentTouching_Adjacent'],
                        second_closest=Object.find_or_create_by(
                            session=session,
                            image=Image.find_by(
                                session=session,
                                description=int(row['ImageNumber'])
                            ),
                            description=row['Neighbors_SecondClosestObjectNumber_5']
                        ),
                        second_closest_distance_5=row['Neighbors_SecondClosestDistance_5'],
                        second_closest_distance_adjacent=row['Neighbors_SecondClosestDistance_Adjacent'],
                        second_closest_object_number_adjacent=row['Neighbors_SecondClosestObjectNumber_Adjacent']
                    )

                    neighborhood.object = object

                    match.neighborhood = neighborhood
                except KeyError:
                    logger.debug(KeyError)

                match.pattern = pattern

                match.shape = shape

                for correlation_column in correlation_columns:
                    dependent, independent = correlation_column

                    correlation = Correlation(
                        coefficient=row[
                            'Correlation_Correlation_{}_{}'.format(
                                dependent.description,
                                independent.description
                            )
                        ]
                    )

                    correlation.dependent = dependent

                    correlation.independent = independent

                    match.correlations.append(correlation)

                for channel in channels:
                    intensity = Intensity(
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

                    intensity.channel = channel

                    match.intensities.append(intensity)

                    edge = Edge(
                        integrated=row[
                            'Intensity_IntegratedIntensityEdge_{}'.format(
                                channel.description
                            )
                        ],
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

                    edge.channel = channel

                    match.edges.append(edge)

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

                    location.channel = channel

                    match.locations.append(location)

                    for scale in scales:
                        texture = Texture(
                            angular_second_moment=row[
                                'Texture_AngularSecondMoment_{}_{}_0'.format(
                                    channel.description,
                                    scale
                                )
                            ],
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

                        texture.channel = channel

                        match.textures.append(texture)

                    for count in counts:
                        radial_distribution = RadialDistribution(
                            bins=count,
                            frac_at_d=row[
                                'RadialDistribution_FracAtD_{}_{}of4'.format(
                                    channel.description,
                                    count
                                )
                            ],
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

                        radial_distribution.channel = channel

                        match.radial_distributions.append(radial_distribution)

        session.commit()
