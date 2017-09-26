import setuptools


setuptools.setup(
    name='cytominer_database',
    version='0.0.0',
    author=[
        'Allen Goodman',
        "Claire McQuin",
        "Shantanu Singh"
    ],
    author_email=[
        "allen.goodman@icloud.com",
        "mcquincl@gmail.com",
        "shsingh@broadinstitute.org"
    ],
    extras_require={
        "doc": [
            "sphinx>=1.6.3"
        ],
        "test": [
            "pytest>=3.2.2"
        ]
    },
    long_description="cytominer-database provides mechanisms to import CSV "
                     "files generated in a morphological profiling experiment "
                     "into a database backend. "
                     "Please refer to the online documentation at "
                     "http://cytominer-database.readthedocs.io",
    package_data={
        'cytominer_database': [
            'config/*.ini',
            'config/*.json',
            'config/*.sql'
        ],
    },
    packages=setuptools.find_packages(
        exclude=[
            'tests',
            'doc'
        ]
    ),
    include_package_data=True,
    install_requires=[
        'backports.tempfile>=1.0rc1',
        'click>=6.7',
        'configparser>=3.5.0',
        'csvkit>=1.0.2',
        'networkx<2.0',  # TODO: https://github.com/blaze/odo/issues/579 -- remove dependency when resolved
        'odo>=0.5.0',
        'pandas>=0.20.3'
    ],
    entry_points={
        'console_scripts': [
            'ingest=cytominer_database.ingest:__main__',
        ]
    },
    license='BSD',
    url='https://github.com/cytomining/cytominer-database'
)
