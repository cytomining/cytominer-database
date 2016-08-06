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

    def is_valid(f):
        nrows = sum(1 for line in open(f)) - 1
        file_size = os.stat(f).st_size
        return (file_size > 0) & (nrows >= 1)

    file_checks = dict({(filename, is_valid(filename)) for filename in [*pattern_csvs, image_csv]})

    if not all(file_checks.values()):
        invalid_files = ",".join([os.path.basename(filename) for (filename, valid) in file_checks.items() if not valid])
        raise OSError("Some files were invalid: {}. Skipping {}.".format(invalid_files, directory))

    return pattern_csvs, image_csv
