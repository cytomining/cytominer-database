from perturbation.models import *
from perturbation.sqlite3 import *
import click
import collections
import glob
import os
import pandas
import perturbation.base
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.event
import sqlalchemy.orm
import sqlite3
import uuid


def find_directories(directory):
    """

    :param directory:

    :return:

    """

    directories = []

    for filename in glob.glob(os.path.join(directory, '*')):
        directories.append(os.path.relpath(filename))

    return set(directories)


def find_channel_by(dictionaries, description):
    """

    :param dictionaries:
    :param description:

    :return:

    """

    for dictionary in dictionaries:
        if dictionary['description'] == description:
            return dictionary['id']


def find_image_by(dictionaries, description):
    """

    :param dictionaries:
    :param description:

    :return:

    """

    for dictionary in dictionaries:
        if dictionary['description'] == description:
            return dictionary['id']


def find_object_by(description, dictionaries, image_id):
    """

    :param description:
    :param dictionaries:
    :param image_id:

    :return:

    """

    for dictionary in dictionaries:
        if (dictionary['description'] == description) and (dictionary['image_id'] == image_id):
            return dictionary['id']


def create(backend_file_path):
    connection = sqlite3.connect(backend_file_path)

    connection.create_aggregate('standard_deviation', 1, StandardDeviation)

    connection.create_function('standard_score', 3, standardize)

    return connection


def seed(input, output, verbose=False):
    engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.realpath(output)),
                                      creator=lambda: create(os.path.realpath(output)))

    session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = session()

    perturbation.base.Base.metadata.create_all(engine)

    channel_dictionaries = []

    for directory in find_directories(input):
        coordinate_dictionaries = []

        correlation_dictionaries = []

        edge_dictionaries = []

        image_dictionaries = []

        intensity_dictionaries = []

        location_dictionaries = []

        match_dictionaries = []

        moment_dictionaries = []

        neighborhood_dictionaries = []

        object_dictionaries = []

        plate_dictionaries = []

        radial_distribution_dictionaries = []

        shape_dictionaries = []

        texture_dictionaries = []

        well_dictionaries = []

        data = pandas.read_csv(os.path.join(directory, 'image.csv'))

        barcodes = data['Metadata_Barcode'].unique()

        for barcode in barcodes:
            plate_dictionary = {
                'barcode': str(int(barcode)),
                'id': uuid.uuid4()
            }

            plate_dictionaries.append(plate_dictionary)

            well_descriptions = data[data['Metadata_Barcode'] == barcode]['Metadata_Well'].unique()

            for well_description in well_descriptions:
                well_dictionary = {
                    'description': well_description,
                    'id': uuid.uuid4(),
                    'plate_id': plate_dictionary['id']
                }

                well_dictionaries.append(well_dictionary)

                image_descriptions = data[
                    (data['Metadata_Barcode'] == barcode) & (data['Metadata_Well'] == well_description)
                ]['ImageNumber'].unique()

                for image_description in image_descriptions:
                    image_dictionary = {
                        'description': int(image_description),
                        'id': uuid.uuid4(),
                        'well_id': well_dictionary['id']
                    }

                    image_dictionaries.append(image_dictionary)

        # TODO: Read only the header, and read all the patterns because some columns are present in one and not the other
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        def get_object_numbers(s):
            return data[['ImageNumber', s]].rename(columns={s: 'ObjectNumber'}).drop_duplicates()

        object_numbers = pandas.concat(
            [get_object_numbers(s) for s in ['ObjectNumber',
                                             'Neighbors_FirstClosestObjectNumber_5',
                                             'Neighbors_SecondClosestObjectNumber_5'
                                             ]]
        ).drop_duplicates()

        for index, object_number in object_numbers.iterrows():
            object_dictionary = {
                'description': str(
                    object_number[
                        'ObjectNumber'
                    ]
                ),
                'id': uuid.uuid4(),
                'image_id': find_image_by(
                    description=object_number[
                        'ImageNumber'
                    ],
                    dictionaries=image_dictionaries
                )
            }

            object_dictionaries.append(object_dictionary)

        session.commit()

        filenames = []

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [os.path.join(directory, 'image.csv'), os.path.join(directory, 'object.csv')]:
                filenames.append(os.path.basename(filename))

        pattern_descriptions = []

        for filename in filenames:
            pattern_descriptions.append(filename.split('.')[0])

        patterns = []

        for pattern_description in pattern_descriptions:
            pattern = Pattern.find_or_create_by(
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
            channel_dictionary = find_channel_by(channel_dictionaries, channel_description)

            if not channel_dictionary:
                channel_dictionary = {
                    'description': channel_description,
                    'id': uuid.uuid4()
                }

                channel_dictionaries.append(channel_dictionary)

        for column in columns:
            split_columns = column.split('_')

            if split_columns[0] == 'Correlation':
                for channel_dictionary in channel_dictionaries:
                    if channel_dictionary['description'] == split_columns[2]:
                        a = channel_dictionary

                    if channel_dictionary['description'] == split_columns[3]:
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
            data = pandas.read_csv(os.path.join(directory, '{}.csv').format(pattern.description))

            with click.progressbar(length=data.shape[0], label="Processing " + pattern.description, show_eta=True) as bar:
                for index, row in data.iterrows():
                    bar.update(1)

                    row = collections.defaultdict(lambda: None, row)

                    image_id = find_image_by(
                        description=row[
                            'ImageNumber'
                        ],
                        dictionaries=image_dictionaries
                    )

                    object_id = find_object_by(
                        description=str(int(
                            row[
                                'ObjectNumber'
                            ]
                        )),
                        image_id=image_id,
                        dictionaries=object_dictionaries
                    )

                    center = {
                        'abscissa': row[
                            'Location_Center_X'
                        ],
                        'id': uuid.uuid4(),
                        'ordinate': row[
                            'Location_Center_Y'
                        ]
                    }

                    coordinate_dictionaries.append(center)

                    neighborhood_dictionary = {
                        'angle_between_neighbors_5': row[
                            'Neighbors_AngleBetweenNeighbors_5'
                        ],
                        'angle_between_neighbors_adjacent': row[
                            'Neighbors_AngleBetweenNeighbors_Adjacent'
                        ],
                        'first_closest_distance_5': row[
                            'Neighbors_FirstClosestDistance_5'
                        ],
                        'first_closest_distance_adjacent': row[
                            'Neighbors_FirstClosestDistance_Adjacent'
                        ],
                        'first_closest_object_number_adjacent': row[
                            'Neighbors_FirstClosestObjectNumber_Adjacent'
                        ],
                        'id': uuid.uuid4(),
                        'number_of_neighbors_5': row[
                            'Neighbors_NumberOfNeighbors_5'
                        ],
                        'number_of_neighbors_adjacent': row[
                            'Neighbors_NumberOfNeighbors_Adjacent'
                        ],
                        'object_id': object_id,
                        'percent_touching_5': row[
                            'Neighbors_PercentTouching_5'
                        ],
                        'percent_touching_adjacent': row[
                            'Neighbors_PercentTouching_Adjacent'
                        ],
                        'second_closest_distance_5': row[
                            'Neighbors_SecondClosestDistance_5'
                        ],
                        'second_closest_distance_adjacent': row[
                            'Neighbors_SecondClosestDistance_Adjacent'
                        ],
                        'second_closest_object_number_adjacent': row[
                            'Neighbors_SecondClosestObjectNumber_Adjacent'
                        ]
                    }

                    if row['Neighbors_FirstClosestObjectNumber_5']:
                        description = str(int(row['Neighbors_FirstClosestObjectNumber_5']))

                        closest_id = find_object_by(
                            description=description,
                            image_id=image_id,
                            dictionaries=object_dictionaries
                        )

                        neighborhood_dictionary['closest_id'] = closest_id

                    if row['Neighbors_SecondClosestObjectNumber_5']:
                        description = str(int(row['Neighbors_SecondClosestObjectNumber_5']))

                        second_closest_id = find_object_by(
                            description=description,
                            image_id=image_id,
                            dictionaries=object_dictionaries
                        )

                        neighborhood_dictionary['second_closest_id'] = second_closest_id

                    neighborhood_dictionaries.append(neighborhood_dictionary)

                    shape_center = {
                        'abscissa': row[
                            'AreaShape_Center_X'
                        ],
                        'id': uuid.uuid4(),
                        'ordinate': row[
                            'AreaShape_Center_Y'
                        ]
                    }

                    coordinate_dictionaries.append(shape_center)

                    shape = {
                        'area': row[
                            'AreaShape_Area'
                        ],
                        'center_id': shape_center[
                            'id'
                        ],
                        'compactness': row[
                            'AreaShape_Compactness'
                        ],
                        'eccentricity': row[
                            'AreaShape_Eccentricity'
                        ],
                        'euler_number': row[
                            'AreaShape_EulerNumber'
                        ],
                        'extent': row[
                            'AreaShape_Extent'
                        ],
                        'form_factor': row[
                            'AreaShape_FormFactor'
                        ],
                        'id': uuid.uuid4(),
                        'major_axis_length': row[
                            'AreaShape_MajorAxisLength'
                        ],
                        'max_feret_diameter': row[
                            'AreaShape_MaxFeretDiameter'
                        ],
                        'maximum_radius': row[
                            'AreaShape_MaximumRadius'
                        ],
                        'mean_radius': row[
                            'AreaShape_MeanRadius'
                        ],
                        'median_radius': row[
                            'AreaShape_MedianRadius'
                        ],
                        'min_feret_diameter': row[
                            'AreaShape_MinFeretDiameter'
                        ],
                        'minor_axis_length': row[
                            'AreaShape_MinorAxisLength'
                        ],
                        'orientation': row[
                            'AreaShape_Orientation'
                        ],
                        'perimeter': row[
                            'AreaShape_Perimeter'
                        ],
                        'solidity': row[
                            'AreaShape_Solidity'
                        ]
                    }

                    shape_dictionaries.append(shape)

                    for a, b in moments:
                        moment_dictionary = {
                            'a': a,
                            'b': b,
                            'id': uuid.uuid4(),
                            'score': row[
                                'AreaShape_Zernike_{}_{}'.format(
                                    a,
                                    b
                                )
                            ],
                            'shape_id': shape[
                                'id'
                            ]
                        }

                        moment_dictionaries.append(moment_dictionary)

                    match = {
                        'center_id': center[
                            'id'
                        ],
                        'id': uuid.uuid4(),
                        'neighborhood_id': neighborhood_dictionary[
                            'id'
                        ],
                        'object_id': object_id,
                        'pattern_id': pattern.id,
                        'shape_id': shape[
                            'id'
                        ]
                    }

                    match_dictionaries.append(match)

                    for dependent, independent in correlation_columns:
                        correlation_dictionary = {
                            'coefficient': row[
                                'Correlation_Correlation_{}_{}'.format(
                                    dependent[
                                        'description'
                                    ],
                                    independent[
                                        'description'
                                    ]
                                )
                            ],
                            'dependent_id': dependent[
                                'id'
                            ],
                            'id': uuid.uuid4(),
                            'independent_id': independent[
                                'id'
                            ],
                            'match_id': match[
                                'id'
                            ]
                        }

                        correlation_dictionaries.append(correlation_dictionary)

                    for channel_dictionary in channel_dictionaries:
                        intensity_dictionary = {
                            'channel_id': channel_dictionary[
                                'id'
                            ],
                            'first_quartile': row[
                                'Intensity_LowerQuartileIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'id': uuid.uuid4(),
                            'integrated': row[
                                'Intensity_IntegratedIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'mass_displacement': row[
                                'Intensity_MassDisplacement_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'match_id': match[
                                'id'
                            ],
                            'maximum': row[
                                'Intensity_MaxIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'mean': row[
                                'Intensity_MeanIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'median': row[
                                'Intensity_MedianIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'median_absolute_deviation': row[
                                'Intensity_MADIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'minimum': row[
                                'Intensity_MinIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'standard_deviation': row[
                                'Intensity_StdIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'third_quartile': row[
                                'Intensity_UpperQuartileIntensity_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ]
                        }

                        intensity_dictionaries.append(intensity_dictionary)

                        edge = {
                            'channel_id': channel_dictionary[
                                'id'
                            ],
                            'id': uuid.uuid4(),
                            'integrated': row[
                                'Intensity_IntegratedIntensityEdge_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'match_id': match[
                                'id'
                            ],
                            'maximum': row[
                                'Intensity_MaxIntensityEdge_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'mean': row[
                                'Intensity_MeanIntensityEdge_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'minimum': row[
                                'Intensity_MinIntensityEdge_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'standard_deviation': row[
                                'Intensity_StdIntensityEdge_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ]
                        }

                        edge_dictionaries.append(edge)

                        center_mass_intensity = {
                            'abscissa': row[
                                'Location_CenterMassIntensity_X_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'id': uuid.uuid4(),
                            'ordinate': row[
                                'Location_CenterMassIntensity_Y_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ]
                        }

                        coordinate_dictionaries.append(center_mass_intensity)

                        max_intensity = {
                            'abscissa': row[
                                'Location_MaxIntensity_X_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ],
                            'id': uuid.uuid4(),
                            'ordinate': row[
                                'Location_MaxIntensity_Y_{}'.format(
                                    channel_dictionary[
                                        'description'
                                    ]
                                )
                            ]
                        }

                        coordinate_dictionaries.append(max_intensity)

                        location = {
                            'center_mass_intensity_id': center_mass_intensity[
                                'id'
                            ],
                            'channel_id': channel_dictionary[
                                'id'
                            ],
                            'match_id': match[
                                'id'
                            ],
                            'max_intensity_id': max_intensity[
                                'id'
                            ]
                        }

                        location_dictionaries.append(location)

                        for scale in scales:
                            texture_dictionary = {
                                'angular_second_moment': row[
                                    'Texture_AngularSecondMoment_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'channel_id': channel_dictionary[
                                    'id'
                                ],
                                'contrast': row[
                                    'Texture_Contrast_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'correlation': row[
                                    'Texture_Correlation_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'difference_entropy': row[
                                    'Texture_DifferenceEntropy_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'difference_variance': row[
                                    'Texture_DifferenceVariance_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'id': uuid.uuid4(),
                                'match_id': match[
                                    'id'
                                ],
                                'scale': scale,
                                'entropy': row[
                                    'Texture_Entropy_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'gabor': row[
                                    'Texture_Gabor_{}_{}'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'info_meas_1': row[
                                    'Texture_InfoMeas1_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'info_meas_2': row[
                                    'Texture_InfoMeas2_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'inverse_difference_moment': row[
                                    'Texture_InverseDifferenceMoment_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'sum_average': row[
                                    'Texture_SumAverage_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'sum_entropy': row[
                                    'Texture_SumEntropy_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'sum_variance': row[
                                    'Texture_SumVariance_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ],
                                'variance': row[
                                    'Texture_Variance_{}_{}_0'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        scale
                                    )
                                ]
                            }

                            texture_dictionaries.append(texture_dictionary)

                        for count in counts:
                            radial_distribution_dictionary = {
                                'bins': count,
                                'channel_id': channel_dictionary[
                                    'id'
                                ],
                                'frac_at_d': row[
                                    'RadialDistribution_FracAtD_{}_{}of4'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        count
                                    )
                                ],
                                'id': uuid.uuid4(),
                                'match_id': match[
                                    'id'
                                ],
                                'mean_frac': row[
                                    'RadialDistribution_MeanFrac_{}_{}of4'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        count
                                    )
                                ],
                                'radial_cv': row[
                                    'RadialDistribution_RadialCV_{}_{}of4'.format(
                                        channel_dictionary[
                                            'description'
                                        ],
                                        count
                                    )
                                ]
                            }

                            radial_distribution_dictionaries.append(radial_distribution_dictionary)

        session.bulk_insert_mappings(
            Coordinate,
            coordinate_dictionaries
        )

        session.bulk_insert_mappings(
            Correlation,
            correlation_dictionaries
        )

        session.bulk_insert_mappings(
            Edge,
            edge_dictionaries
        )

        session.bulk_insert_mappings(
            Image,
            image_dictionaries
        )

        session.bulk_insert_mappings(
            Intensity,
            intensity_dictionaries
        )

        session.bulk_insert_mappings(
            Location,
            location_dictionaries
        )

        session.bulk_insert_mappings(
            Match,
            match_dictionaries
        )

        session.bulk_insert_mappings(
            Moment,
            moment_dictionaries
        )

        session.bulk_insert_mappings(
            Neighborhood,
            neighborhood_dictionaries
        )

        session.bulk_insert_mappings(
            Object,
            object_dictionaries
        )

        session.bulk_insert_mappings(
            Plate,
            plate_dictionaries
        )

        session.bulk_insert_mappings(
            RadialDistribution,
            radial_distribution_dictionaries
        )

        session.bulk_insert_mappings(
            Shape,
            shape_dictionaries
        )

        session.bulk_insert_mappings(
            Texture,
            texture_dictionaries
        )

        session.bulk_insert_mappings(
            Well,
            well_dictionaries
        )

        session.commit()

    session.bulk_insert_mappings(
        Channel,
        channel_dictionaries
    )

    session.commit()
