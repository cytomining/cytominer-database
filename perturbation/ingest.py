import hashlib
import logging
import odo 
import os
import perturbation.utils

logger = logging.getLogger(__name__)

def seed(config, input, output):
    """Call functions to create backend

    """
    logger.debug(input)
    logger.debug(output)

    pathnames = perturbation.utils.find_directories(input)

    for directory in pathnames:
        try:
            image_csv = os.path.join(directory, config["filenames"]["image"])
            assert os.path.isfile(image_csv)
        except AssertionError:
            logger.debug("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))
            continue

        print("Ingesting {} into {}".format(image_csv, output))
        odo.odo(image_csv, "{}::image".format(output), has_header=True, delimiter=",")