import glob
import os

def find_directories(directory):
    directories = set()

    filenames = glob.glob(os.path.join(directory, '*/'))

    for filename in filenames:
        pathname = os.path.relpath(filename)

        directories.add(pathname)

    return directories