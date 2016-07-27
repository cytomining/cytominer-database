import glob
import hashlib
import os
import pandas
import perturbation.models
import perturbation.utils
import logging

logger = logging.getLogger(__name__)

def seed(directories, scoped_session):
    """Creates backend

    :param directories: top-level directory containing sub-directories, each of which have an image.csv and object.csv
    :return: None
    """
    pathnames = perturbation.utils.find_directories(directories)

    for directory in pathnames:
        try:
            data = pandas.read_csv(os.path.join(directory, 'image.csv'))
        except OSError:
            continue

        digest = hashlib.md5(open(os.path.join(directory, 'image.csv'), 'rb').read()).hexdigest()

        # Populate plates, wells, images, qualities

        data['Metadata_Barcode'] = data['Metadata_Barcode'].astype(str)

        data['ImageNumber'] = data['ImageNumber'].astype(str)

        data['digest_ImageNumber'] = data['ImageNumber'].apply(lambda x: '{}_{}'.format(digest, x))

        # TODO: 'Metadata_Barcode' should be gotten from a config file
        plate_descriptions = data['Metadata_Barcode'].unique()

        plates = find_plates(plate_descriptions, scoped_session)

        for plate in plates:
            # TODO: 'Metadata_Barcode' should be gotten from a config file
            well_descriptions = data[data['Metadata_Barcode'] == plate.description]['Metadata_Well'].unique()

            wells = find_wells(well_descriptions, plate, scoped_session)

            for well in wells:
                image_descriptions = data[(data['Metadata_Barcode'] == plate.description) & (data['Metadata_Well'] == well.description)]['digest_ImageNumber'].unique()

                images = find_images(image_descriptions, well, scoped_session)

                for image in images:
                    # TODO: Change find_or_create_by to create
                    # TODO: 'Metadata_*' should be gotten from a config file
                    quality = perturbation.models.Quality.find_or_create_by(
                            session=scoped_session,
                            image=image,
                            count_cell_clump=int(data.loc[data['digest_ImageNumber'] == image.description, 'Metadata_isCellClump']),
                            count_debris=int(data.loc[data['digest_ImageNumber'] == image.description, 'Metadata_isDebris']),
                            count_low_intensity=int(data.loc[data['digest_ImageNumber'] == image.description, 'Metadata_isLowIntensity'])
                    )

        filenames = []

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [os.path.join(directory, 'image.csv'), os.path.join(directory, 'object.csv')]:
                filenames.append(os.path.basename(filename))

        pattern_descriptions = find_pattern_descriptions(filenames)

        _ = find_patterns(pattern_descriptions, scoped_session)

        # TODO: Read all the patterns (not just Cells.csv; note that some datasets may not have Cells as a pattern)
        data = pandas.read_csv(os.path.join(directory, 'Cells.csv'))

        columns = data.columns

        channel_descriptions = find_channel_descriptions(columns)

        _ = find_channels(channel_descriptions, scoped_session)

        scoped_session.commit()


def find_channel_descriptions(columns):
    channel_descriptions = set()

    for column in columns:
        split_columns = column.split("_")

        if split_columns[0] == "Intensity":
            channel_description = split_columns[2]

            channel_descriptions.add(channel_description)

    return channel_descriptions


def find_channels(channel_descriptions, session):
    channels = []

    for channel_description in channel_descriptions:
        channel = perturbation.models.Channel.find_or_create_by(
            session=session,
            description=channel_description
        )
        channels.append(channel)

    return channels


def find_images(image_descriptions, well, session):
    images = []

    for image_description in image_descriptions:
        image = perturbation.models.Image.find_or_create_by(
                session=session,
                description=image_description,
                well=well
        )

        images.append(image)

    return images


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


def find_plates(plate_descriptions, session):
    plates = []

    for plate_description in plate_descriptions:
        plate = perturbation.models.Plate.find_or_create_by(
                session=session,
                description=plate_description
        )

        plates.append(plate)

    return plates


def find_wells(well_descriptions, plate, session):
    wells = []

    for well_description in well_descriptions:
        well = perturbation.models.Well.find_or_create_by(
                session=session,
                description=well_description,
                plate=plate
        )

        wells.append(well)

    return wells
