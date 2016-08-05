import glob
import hashlib
import logging
import odo 
import os
import perturbation.utils
import subprocess
import tempfile

logger = logging.getLogger(__name__)

def into(csv_filename, output, table_name, table_number):

    with tempfile.TemporaryDirectory() as temp_dir:
        appended_csv_filename = os.path.join(temp_dir, os.path.basename(csv_filename))

        cmd = "cp {} {}".format(csv_filename, appended_csv_filename)

        subprocess.check_output(cmd, shell=True)

        logger.debug("Ingesting {} into {}::{} with table_number={}".format(appended_csv_filename, output, table_name, table_number))

        odo.odo(appended_csv_filename, "{}::{}".format(output, table_name), has_header=True, delimiter=",")


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

        table_number = hashlib.md5(open(image_csv, 'rb').read()).hexdigest()

        image_table_name = config["filenames"]["image"].split(".")[0]

        into(csv_filename=image_csv, output=output, table_name=image_table_name, table_number=table_number)

        for filename in glob.glob(os.path.join(directory, '*.csv')):
            if filename not in [
            os.path.join(directory, config['filenames']['image']),
            os.path.join(directory, config['filenames']['object']),
            os.path.join(directory, config['filenames']['experiment'])
            ]:
                pattern_csv = filename

                pattern = os.path.basename(pattern_csv).split('.')[0]

                into(csv_filename=pattern_csv, output=output, table_name=pattern, table_number=table_number)





