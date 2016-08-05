import glob
import os


def find_directories(directory):
    directories = set()

    filenames = glob.glob(os.path.join(directory, '*/'))

    for filename in filenames:
        pathname = os.path.relpath(filename)

        directories.add(pathname)

    return directories


def validate_csvs(config, directory):
    image_csv = os.path.join(directory, config["filenames"]["image"])

    if not os.path.isfile(image_csv):
        raise OSError("{} not found in {}. Skipping.".format(config["filenames"]["image"], directory))

    pattern_csvs = [filename for filename in glob.glob(os.path.join(directory, '*.csv')) if filename not in [
        os.path.join(directory, config['filenames']['image']),
        os.path.join(directory, config['filenames']['object']),
        os.path.join(directory, config['filenames']['experiment'])
    ]]

    filesize_checks = dict({(filename, os.stat(filename).st_size > 0) for filename in [*pattern_csvs, image_csv]})

    if not all(filesize_checks.values()):
        empty_files = ",".join([os.path.basename(filename) for (filename, valid) in filesize_checks.items() if not valid])
        raise OSError("Some files were empty: {}. Skipping {}.".format(empty_files, directory))

    return pattern_csvs, image_csv
