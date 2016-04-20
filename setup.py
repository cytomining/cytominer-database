import Cython.Build
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
    # ext_modules=Cython.Build.cythonize(
    #     "perturbation/*.pyx"
    # ),
    packages=setuptools.find_packages(
        exclude=[
            'test'
        ]
    ),
    install_requires=[
        'click',
        'csvkit',
        'pandas',
        'pytest',
        'sqlalchemy',
        'sqlparse'
    ],
    setup_requires=[
        'Cython'
    ],
    entry_points={
        'console_scripts': [
            'perturbation=perturbation:__main__',
        ]
    }
)
