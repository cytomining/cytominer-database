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
    name='perturbation',
    version='0.0.0',
    author='Allen Goodman',
    author_email='allen.goodman@icloud.com',
    cmdclass={
        'test': Test
    },
    package_data={
        'perturbation' :['config/*.ini', 'scripts/*.sh'],
    },
    packages=setuptools.find_packages(
        exclude=[
            'test'
        ]
    ),
    include_package_data=True,
    install_requires=[
        'click',
        'csvkit',
        'odo',
        'pandas',
        'psycopg2',
        'pytest',
        'sqlalchemy',
        'sqlparse'
    ],
    entry_points={
        'console_scripts': [
            'perturbation=perturbation:__main__',
            'ingest=perturbation.ingest:main',
            'inspect=perturbation.inspect:main'
        ]
    }
)
