"""

"""

import click
import glob
import os
import IPython
import pandas
import perturbation.base
import sqlalchemy.exc
from perturbation.models import *
from perturbation.sqlite3 import *
import sqlalchemy
import sqlite3
import sqlalchemy.orm
import logging


def seed(input, output, verbose=False):
    def create(backend_file_path):
        connection = sqlite3.connect(backend_file_path)

        connection.create_aggregate('standard_deviation', 1, StandardDeviation)

        connection.create_function('standard_score', 3, standardize)

        return connection

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


    def find_channels(channel_descriptions, session):
        channels = []
        for channel_description in channel_descriptions:
            channel = Channel.find_or_create_by(
                session,
                description=channel_description
            )

            channels.append(channel)
        return channels


    def find_channel_descriptions(columns):
        channel_descriptions = []
        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'Intensity':
                channel_descriptions.append(split_columns[2])
        channel_descriptions = set(channel_descriptions)
        return channel_descriptions


    def find_correlation_columns(columns, session):
        correlation_columns = []
        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'Correlation':
                a = Channel.find_or_create_by(session, description=split_columns[2])
                b = Channel.find_or_create_by(session, description=split_columns[3])

                correlation_columns.append((a, b))
        return correlation_columns


    def find_patterns(pattern_descriptions, session):
        patterns = []

        for pattern_description in pattern_descriptions:
            pattern = Pattern.find_or_create_by(session, description=pattern_description)

            patterns.append(pattern)

        return patterns


    def find_pattern_descriptions(filenames):
        pattern_descriptions = []

        for filename in filenames:
            pattern_descriptions.append(filename.split('.')[0])

        return pattern_descriptions


    def find_moment_columns(columns):
        moments = []
        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'AreaShape' and split_columns[1] == 'Zernike':
                moments.append((split_columns[2], split_columns[3]))
        return moments

    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)), creator=lambda: create(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

    for chunk in pandas.read_csv(os.path.join(input, 'image.csv'), chunksize=4):
        for index, row in chunk.iterrows():
            well = Well.find_or_create_by(
                session=session,
                description=row[
                    'Metadata_Well'
                ]
            )

            plate = Plate.find_or_create_by(
                session=session,
                barcode=row[
                    'Metadata_Barcode'
                ]
            )

            plate.wells.append(well)

            image = Image.find_or_create_by(
                session=session,
                id=row[
                    'ImageNumber'
                ]
            )

            well.images.append(image)

    filenames = []

    for filename in glob.glob(os.path.join(input, '*.csv')):
        if filename not in [os.path.join(input, 'image.csv'), os.path.join(input, 'object.csv')]:
            filenames.append(os.path.basename(filename))

    pattern_descriptions = find_pattern_descriptions(filenames)

    patterns = find_patterns(pattern_descriptions, session)

    data = pandas.read_csv(os.path.join(input, 'Cells.csv'))

    columns = data.columns

    correlation_columns = find_correlation_columns(columns, session)

    channels = find_channels(find_channel_descriptions(columns), session)

    scales = find_scales(columns)

    counts = find_counts(columns)

    objects = create_objects(input, 'Cells', session)

    objects = session.query(Object).all()

    for pattern in patterns:
        data = pandas.read_csv(os.path.join(input, '{}.csv').format(pattern.description))

        for index, record in data.iterrows():
            object = find_object(record['ObjectNumber'], record['ImageNumber'], objects)

            match = create_match(channels, columns, find_moment_columns, pattern, record, object, objects, counts, scales)

            session.add(match)

    session.commit()


def find_object(description, image_id, objects):
    for object in objects:
        if object.description == description and object.image_id == image_id:
            return object


def create_match(channels, columns, find_moment_columns, pattern, record, object, objects, counts, scales):
    try:
        match = Match(
            center=Coordinate(
                abscissa=int(record['Location_Center_X']),
                ordinate=int(record['Location_Center_Y'])
            ),
            correlations=[

            ],
            edges=[
                Edge(
                    channel=channel,
                    integrated=record['Intensity_IntegratedIntensityEdge_{}'.format(channel.description)],
                    maximum=record['Intensity_MaxIntensityEdge_{}'.format(channel.description)],
                    mean=record['Intensity_MeanIntensityEdge_{}'.format(channel.description)],
                    minimum=record['Intensity_MinIntensityEdge_{}'.format(channel.description)],
                    standard_deviation=record['Intensity_StdIntensityEdge_{}'.format(channel.description)]
                ) for channel in channels
            ],
            intensities=[
                Intensity(
                    channel=channel,
                    first_quartile=record['Intensity_LowerQuartileIntensity_{}'.format(channel.description)],
                    integrated=record['Intensity_IntegratedIntensity_{}'.format(channel.description)],
                    mass_displacement=record['Intensity_MassDisplacement_{}'.format(channel.description)],
                    maximum=record['Intensity_MaxIntensity_{}'.format(channel.description)],
                    mean=record['Intensity_MeanIntensity_{}'.format(channel.description)],
                    median=record['Intensity_MedianIntensity_{}'.format(channel.description)],
                    median_absolute_deviation=record['Intensity_MADIntensity_{}'.format(channel.description)],
                    minimum=record['Intensity_MinIntensity_{}'.format(channel.description)],
                    standard_deviation=record['Intensity_StdIntensity_{}'.format(channel.description)],
                    third_quartile=record['Intensity_UpperQuartileIntensity_{}'.format(channel.description)]
                ) for channel in channels
            ],
            locations=[
                Location(
                    center_mass_intensity=Coordinate(
                        abscissa=int(record['Location_CenterMassIntensity_X_{}'.format(channel.description)]),
                        ordinate=int(record['Location_CenterMassIntensity_Y_{}'.format(channel.description)])
                    ),
                    channel=channel,
                    max_intensity=Coordinate(
                        abscissa=int(record['Location_MaxIntensity_X_{}'.format(channel.description)]),
                        ordinate=int(record['Location_MaxIntensity_Y_{}'.format(channel.description)])
                    )
                ) for channel in channels
            ],
            # neighborhood=Neighborhood(
            #     angle_between_neighbors_5=record['Neighbors_AngleBetweenNeighbors_5'],
            #     angle_between_neighbors_adjacent=record['Neighbors_AngleBetweenNeighbors_Adjacent'],
            #     closest_id=find_object(record['Neighbors_FirstClosestObjectNumber_5'], record['ImageNumber'], objects),
            #     first_closest_distance_5=float(record['Neighbors_FirstClosestDistance_5']),
            #     first_closest_distance_adjacent=float(record['Neighbors_FirstClosestDistance_Adjacent']),
            #     first_closest_object_number_adjacent=float(record['Neighbors_FirstClosestObjectNumber_Adjacent']),
            #     number_of_neighbors_5=float(record['Neighbors_NumberOfNeighbors_5']),
            #     number_of_neighbors_adjacent=float(record['Neighbors_NumberOfNeighbors_Adjacent']),
            #     object_id=object.id,
            #     percent_touching_5=float(record['Neighbors_PercentTouching_5']),
            #     percent_touching_adjacent=float(record['Neighbors_PercentTouching_Adjacent']),
            #     second_closest_distance_5=float(record['Neighbors_SecondClosestDistance_5']),
            #     second_closest_distance_adjacent=float(record['Neighbors_SecondClosestDistance_Adjacent']),
            #     second_closest_id=0,
            #     second_closest_object_number_adjacent=float(record['Neighbors_SecondClosestObjectNumber_Adjacent'])
            # ),
            object_id=object.id,
            pattern_id=pattern.id,
            radial_distributions=[

            ],
            shape=Shape(
                area=record['AreaShape_Area'],
                center=Coordinate(
                    abscissa=int(record['AreaShape_Center_X']),
                    ordinate=int(record['AreaShape_Center_Y'])
                ),
                compactness=record['AreaShape_Compactness'],
                eccentricity=record['AreaShape_Eccentricity'],
                euler_number=record['AreaShape_EulerNumber'],
                extent=record['AreaShape_Extent'],
                form_factor=record['AreaShape_FormFactor'],
                major_axis_length=record['AreaShape_MajorAxisLength'],
                max_feret_diameter=record['AreaShape_MaxFeretDiameter'],
                maximum_radius=record['AreaShape_MaximumRadius'],
                moments=[
                    Moment(
                        a=moment[0],
                        b=moment[1],
                        score=record['AreaShape_Zernike_{}_{}'.format(moment[0], moment[1])]
                    ) for moment in (find_moment_columns(columns))
                    ],
                mean_radius=record['AreaShape_MeanRadius'],
                median_radius=record['AreaShape_MedianRadius'],
                min_feret_diameter=record['AreaShape_MinFeretDiameter'],
                minor_axis_length=record['AreaShape_MinorAxisLength'],
                orientation=record['AreaShape_Orientation'],
                perimeter=record['AreaShape_Perimeter'],
                solidity=record['AreaShape_Solidity']
            ),
            textures=[

            ]
        )

        for channel in channels:
            for count in counts:
                radial_distribution = RadialDistribution(
                    # bins=count,
                    # channel=channel,
                    # frac_at_d=record['RadialDistribution_FracAtD_{}_{}of4'.format(channel.description, count)],
                    # mean_frac=record['RadialDistribution_MeanFrac_{}_{}of4'.format(channel.description, count)],
                    # radial_cv=record['RadialDistribution_RadialCV_{}_{}of4'.format(channel.description, count)]
                )

                # match.radial_distributions.append(radial_distribution)

            for scale in scales:
                texture = Texture(
                    # angular_second_moment=record['Texture_AngularSecondMoment_{}_{}_0'.format(channel.description, scale)],
                    # channel=channel,
                    # contrast=record['Texture_Contrast_{}_{}_0'.format(channel.description, scale)],
                    # correlation=record['Texture_Correlation_{}_{}_0'.format(channel.description, scale)],
                    # difference_entropy=record['Texture_DifferenceEntropy_{}_{}_0'.format(channel.description, scale)],
                    # difference_variance=record['Texture_DifferenceVariance_{}_{}_0'.format(channel.description, scale)],
                    # entropy=record['Texture_Entropy_{}_{}_0'.format(channel.description, scale)],
                    # gabor=record['Texture_Gabor_{}_{}'.format(channel.description, scale)],
                    # info_meas_1=record['Texture_InfoMeas1_{}_{}_0'.format(channel.description, scale)],
                    # info_meas_2=record['Texture_InfoMeas2_{}_{}_0'.format(channel.description, scale)],
                    # inverse_difference_moment=record['Texture_InverseDifferenceMoment_{}_{}_0'.format(channel.description, scale)],
                    # scale=scale,
                    # sum_average=record['Texture_SumAverage_{}_{}_0'.format(channel.description, scale)],
                    # sum_entropy=record['Texture_SumEntropy_{}_{}_0'.format(channel.description, scale)],
                    # sum_variance=record['Texture_SumVariance_{}_{}_0'.format(channel.description, scale)],
                    # variance=record['Texture_Variance_{}_{}_0'.format(channel.description, scale)]
                )

                # match.textures.append(texture)

        # for correlation_column in correlation_columns:
        #     dependent, independent = correlation_column
        #
        #     name = 'Correlation_Correlation_{}_{}'.format(dependent.description, independent.description)
        #
        #     correlation = Correlation(coefficient=record[name], dependent=dependent, independent=independent)
        #
        #     match.correlations.append(correlation)

        # match.neighborhood.closest = Object.find_or_create_by(
        #     session=session,
        #     description=row['Neighbors_FirstClosestObjectNumber_5'],
        #     image_id=row['ImageNumber']
        # )

        # match.neighborhood.second_closest = Object.find_or_create_by(
        #     session=session,
        #     description=row['Neighbors_SecondClosestObjectNumber_5'],
        #     image_id=row['ImageNumber']
        # )
    except KeyError:
        pass


def create_objects(input, pattern, session):
    data = pandas.read_csv(os.path.join(input, '{}.csv').format(pattern))

    object_records = data[['ImageNumber', 'ObjectNumber']]

    objects = [Object.find_or_create_by(session=session, description=int(x[1]), image_id=int(x[0])) for x in object_records.values]

    return objects