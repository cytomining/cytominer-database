import setuptools


setuptools.setup(
    name='cytominer_database',
    version='0.3.3',
    author="Shantanu Singh",
    author_email="shsingh@broadinstitute.org",
    maintainer="Gregory Way",
    maintainer_email="gregory.way@gmail.com",
    entry_points="""
    [console_scripts]
    cytominer-database=cytominer_database.command:command
    """,
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
        'pandas>=0.20.3',
        'pyarrow>=0.16.1'
    ],
    license='BSD',
    url='https://github.com/cytomining/cytominer-database'
)
