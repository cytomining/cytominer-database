import setuptools

setuptools.setup(
    name='perturbation',
    version='0.0.0',
    author='Allen Goodman',
    author_email='allen.goodman@icloud.com',
    packages=setuptools.find_packages(
            exclude=[
                'test'
            ]
    ),
    install_requires=[
        'pandas',
        'pytest',
        'sqlalchemy'
    ],
    entry_points={
        'console_scripts': [
            'perturbation=perturbation:__main__',
        ]
    }
)
