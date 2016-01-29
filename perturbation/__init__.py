import click
import glob
import os
import IPython
import pandas
import perturbation.base
import perturbation.model.angle
import perturbation.model.coordinate
import perturbation.model.correlation
import perturbation.model.image
import perturbation.model.intensity
import perturbation.model.location
import perturbation.model.match
import perturbation.model.moment
import perturbation.model.neighborhood
import perturbation.model.pattern
import perturbation.model.plate
import perturbation.model.radial_distribution
import perturbation.model.standardized_radial_distribution
import perturbation.model.ring
import perturbation.model.scale
import perturbation.model.shape
import perturbation.model.channel
import perturbation.model.texture
import perturbation.model.well
from perturbation.sqlite3.standard_deviation import StandardDeviation
from perturbation.sqlite3.standard_score import StandardScore
import sqlalchemy
import sqlite3
import sqlalchemy.orm


def create():
    connection = sqlite3.connect('example.sqlite')

    connection.create_aggregate('standard_deviation', 1, StandardDeviation)

    connection.create_aggregate('standard_score', 3, StandardScore)

    return connection


@click.command()
@click.argument('a', nargs=1, type=click.File('rb'))
@click.argument('b', nargs=1, type=click.File('rb'))
def __main__(a, b):
    engine = sqlalchemy.create_engine('sqlite:///example.sqlite', creator=create)

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

    for chunk in pandas.read_csv('test/data/image.csv', chunksize=4):
        for index, row in chunk.iterrows():
            position = perturbation.model.coordinate.Coordinate.find_or_create_by(
                session,
                abscissa=row['Metadata_WellColumn'],
                ordinate=ord(row['Metadata_WellRow'])
            )

            well = perturbation.model.well.Well.find_or_create_by(
                session,
                position=position,
                description=row['Metadata_Well']
            )

            plate = perturbation.model.plate.Plate.find_or_create_by(
                session,
                id=row['Metadata_Site'],
                barcode=row['Metadata_Barcode']
            )

            well.plates.append(plate)

            metadata = perturbation.model.image.Image.find_or_create_by(
                session,
                id=row['ImageNumber'],
                plate_id=plate.id
            )

            plate.images.append(metadata)

    filenames = []

    for filename in glob.glob('test/data/*.csv'):
        if filename not in ['test/data/image.csv', 'test/data/object.csv']:
            filenames.append(os.path.basename(filename))

    pattern_descriptions = []

    for filename in filenames:
        pattern_descriptions.append(filename.split('.')[0])

    patterns = []

    for pattern_description in pattern_descriptions:
        pattern = perturbation.model.pattern.Pattern.find_or_create_by(session, description=pattern_description)

        patterns.append(pattern)

    session.commit()

    data = pandas.read_csv('test/data/cell.csv')

    columns = data.columns

    correlation_columns = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Correlation':
            a = perturbation.model.channel.Channel.find_or_create_by(session, description=split_columns[2])
            b = perturbation.model.channel.Channel.find_or_create_by(session, description=split_columns[3])

            correlation_columns.append((a, b))

    channel_descriptions = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Intensity':
            channel_descriptions.append(split_columns[2])

    channel_descriptions = set(channel_descriptions)

    channels = []

    for channel_description in channel_descriptions:
        channel = perturbation.model.channel.Channel.find_or_create_by(
                session,

                description=channel_description
        )

        channels.append(channel)

    session.commit()

    degrees = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'Texture':
            degrees.append(split_columns[3])

    degrees = set(degrees)

    angles = []

    for degree in degrees:
        angles.append(perturbation.model.angle.Angle.find_or_create_by(session, degree=degree))

    angles = set(angles)

    session.commit()

    ring_ids = []

    for column in columns:
        split_columns = column.split('_')

        if split_columns[0] == 'RadialDistribution':
            ring_ids.append(split_columns[3][0])

    ring_ids = set(ring_ids)

    rings = []

    for ring_id in ring_ids:
        rings.append(perturbation.model.ring.Ring.find_or_create_by(session, id=ring_id))

    rings = set(rings)

    session.commit()

    for pattern in patterns:
        data = pandas.read_csv('test/data/{}.csv'.format(pattern.description))

        for index, row in data.iterrows():
            image = perturbation.model.image.Image.find_or_create_by(session, id=row['ImageNumber'])

            # neighborhood = perturbation.model.neighborhood.Neighborhood.find_or_create_by(
            #         session,
            #         angle_between_neighbors_5=row['Neighbors_AngleBetweenNeighbors_5'],
            #         angle_between_neighbors_adjacent=row['Neighbors_AngleBetweenNeighbors_Adjacent'],
            #         first_closest_distance_5=row['Neighbors_FirstClosestDistance_5'],
            #         first_closest_distance_adjacent=row['Neighbors_FirstClosestDistance_Adjacent'],
            #         first_closest_object_number_5=row['Neighbors_FirstClosestObjectNumber_5'],
            #         first_closest_object_number_adjacent=row['Neighbors_FirstClosestObjectNumber_Adjacent'],
            #         number_of_neighbors_5=row['Neighbors_NumberOfNeighbors_5'],
            #         number_of_neighbors_adjacent=row['Neighbors_NumberOfNeighbors_Adjacent'],
            #         percent_touching_5=row['Neighbors_PercentTouching_5'],
            #         percent_touching_adjacent=row['Neighbors_PercentTouching_Adjacent'],
            #         second_closest_distance_5=row['Neighbors_SecondClosestDistance_5'],
            #         second_closest_distance_adjacent=row['Neighbors_SecondClosestDistance_Adjacent'],
            #         second_closest_object_number_5=row['Neighbors_SecondClosestObjectNumber_5'],
            #         second_closest_object_number_adjacent=row['Neighbors_SecondClosestObjectNumber_Adjacent']
            # )

            shape = perturbation.model.shape.Shape.find_or_create_by(
                session,
                center=perturbation.model.coordinate.Coordinate.find_or_create_by(
                    session,
                    abscissa=row['AreaShape_Center_X'],
                    ordinate=row['AreaShape_Center_Y']
                ),
                area=row['AreaShape_Area'],
                compactness=row['AreaShape_Compactness'],
                eccentricity=row['AreaShape_Compactness'],
                euler_number=row['AreaShape_Compactness'],
                extent=row['AreaShape_Compactness'],
                form_factor=row['AreaShape_Compactness'],
                major_axis_length=row['AreaShape_Compactness'],
                max_feret_diameter=row['AreaShape_Compactness'],
                maximum_radius=row['AreaShape_Compactness'],
                mean_radius=row['AreaShape_Compactness'],
                median_radius=row['AreaShape_Compactness'],
                min_feret_diameter=row['AreaShape_Compactness'],
                minor_axis_length=row['AreaShape_Compactness'],
                orientation=row['AreaShape_Compactness'],
                perimeter=row['AreaShape_Compactness'],
                solidity=row['AreaShape_Compactness']
            )

            match = perturbation.model.match.Match.find_or_create_by(session, id=row['ObjectNumber'])

            match.image = image

            # match.neighborhood = neighborhood

            match.pattern = pattern

            match.shape = shape

            for correlation_column in correlation_columns:
                dependent, independent = correlation_column

                perturbation.model.correlation.Correlation.find_or_create_by(
                    session,
                    dependent=dependent,
                    independent=independent,
                    match=match,
                    coefficient=row[
                        'Correlation_Correlation_{}_{}'.format(dependent.description, independent.description)
                    ]
                )

            for channel in channels:
                perturbation.model.intensity.Intensity.find_or_create_by(
                    session,
                    channel=channel,
                    match=match,
                    integrated_intensity=row['Intensity_IntegratedIntensity_{}'.format(channel.description)],
                    integrated_intensity_edge=row['Intensity_IntegratedIntensityEdge_{}'.format(channel.description)],
                    lower_quartile_intensity=row['Intensity_LowerQuartileIntensity_{}'.format(channel.description)],
                    mad_intensity=row['Intensity_MADIntensity_{}'.format(channel.description)],
                    mass_displacement=row['Intensity_MassDisplacement_{}'.format(channel.description)],
                    max_intensity=row['Intensity_MaxIntensity_{}'.format(channel.description)],
                    max_intensity_edge=row['Intensity_MaxIntensityEdge_{}'.format(channel.description)],
                    mean_intensity=row['Intensity_MeanIntensity_{}'.format(channel.description)],
                    mean_intensity_edge=row['Intensity_MeanIntensityEdge_{}'.format(channel.description)],
                    median_intensity=row['Intensity_MedianIntensity_{}'.format(channel.description)],
                    min_intensity=row['Intensity_MinIntensity_{}'.format(channel.description)],
                    min_intensity_edge=row['Intensity_MinIntensityEdge_{}'.format(channel.description)],
                    std_intensity=row['Intensity_StdIntensity_{}'.format(channel.description)],
                    std_intensity_edge=row['Intensity_StdIntensityEdge_{}'.format(channel.description)],
                    upper_quartile_intensity=row['Intensity_UpperQuartileIntensity_{}'.format(channel.description)]
                )

                perturbation.model.location.Location.find_or_create_by(
                        session,
                        center_mass_intensity=perturbation.model.coordinate.Coordinate.find_or_create_by(
                            session,
                            abscissa=row['Location_CenterMassIntensity_X_{}'.format(channel.description)],
                            ordinate=row['Location_CenterMassIntensity_Y_{}'.format(channel.description)]
                        ),
                        match=match,
                        max_intensity=perturbation.model.coordinate.Coordinate.find_or_create_by(
                            session,
                            abscissa=row['Location_MaxIntensity_X_{}'.format(channel.description)],
                            ordinate=row['Location_MaxIntensity_Y_{}'.format(channel.description)]
                        ),
                        channel=channel
                )

                for angle in angles:
                    perturbation.model.texture.Texture.find_or_create_by(
                        session,
                        angle=angle,
                        match=match,
                        channel=channel,
                        angular_second_moment=row['Texture_AngularSecondMoment_{}_{}_0'.format(channel.description, angle.degree)],
                        contrast=row['Texture_Contrast_{}_{}_0'.format(channel.description, angle.degree)],
                        correlation=row['Texture_Correlation_{}_{}_0'.format(channel.description, angle.degree)],
                        difference_entropy=row['Texture_DifferenceEntropy_{}_{}_0'.format(channel.description, angle.degree)],
                        difference_variance=row['Texture_DifferenceVariance_{}_{}_0'.format(channel.description, angle.degree)],
                        entropy=row['Texture_Entropy_{}_{}_0'.format(channel.description, angle.degree)],
                        gabor=row['Texture_Gabor_{}_{}'.format(channel.description, angle.degree)],
                        info_meas_1=row['Texture_InfoMeas1_{}_{}_0'.format(channel.description, angle.degree)],
                        info_meas_2=row['Texture_InfoMeas2_{}_{}_0'.format(channel.description, angle.degree)],
                        inverse_difference_moment=row['Texture_InverseDifferenceMoment_{}_{}_0'.format(channel.description, angle.degree)],
                        sum_average=row['Texture_SumAverage_{}_{}_0'.format(channel.description, angle.degree)],
                        sum_entropy=row['Texture_SumEntropy_{}_{}_0'.format(channel.description, angle.degree)],
                        sum_variance=row['Texture_SumVariance_{}_{}_0'.format(channel.description, angle.degree)],
                        variance=row['Texture_Variance_{}_{}_0'.format(channel.description, angle.degree)]
                    )

                for ring in rings:
                    perturbation.model.radial_distribution.RadialDistribution.find_or_create_by(
                        session,
                        channel=channel,
                        match=match,
                        ring=ring,
                        frac_at_d=row['RadialDistribution_FracAtD_{}_{}of4'.format(channel.description, ring.id)],
                        mean_frac=row['RadialDistribution_MeanFrac_{}_{}of4'.format(channel.description, ring.id)],
                        radial_cv=row['RadialDistribution_RadialCV_{}_{}of4'.format(channel.description, ring.id)]
                    )

        session.commit()

    IPython.embed()

if __name__ == '__main__':
    __main__()
