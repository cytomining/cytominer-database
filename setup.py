import setuptools
import setuptools.command.test
import sys


class Test(setuptools.command.test.test):
    user_options = [
        ("pytest-args=", "a", "Arguments to pass to py.test")
    ]

    def initialize_options(self):
        setuptools.command.test.test.initialize_options(self)

        self.pytest_args = []

    def finalize_options(self):
        setuptools.command.test.test.finalize_options(self)

        self.test_args = []

        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)

        sys.exit(errno)


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
    cmdclass={
        'test': Test
    },
    package_data={
        'cytominer_database': [
            'config/*.ini',
            'config/*.json',
            'config/*.sql',
            'scripts/*.sh'
        ],
    },
    packages=setuptools.find_packages(
        exclude=[
            'test',
            'doc'
        ]
    ),
    include_package_data=True,
    install_requires=[
        'click>=6.7',
        'csvkit>=1.0.1',
        'odo>=0.5.0',
        'pandas>=0.19.2',
        'pytest>=3.0.6'
    ],
    entry_points={
        'console_scripts': [
            'ingest=cytominer_database.ingest:__main__',
        ]
    },
    license='BSD',
    url='https://github.com/cytomining/cytominer-database'
)
