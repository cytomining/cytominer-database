import setuptools

setuptools.setup(
    name='persistence',
    version='0.0.0',
    author='Allen Goodman',
    author_email='allen.goodman@icloud.com',
    packages=setuptools.find_packages(
            exclude=[

            ]
    ),
    install_requires=[
        'pandas',
        'sqlalchemy'
    ],
    entry_points={
        'console_scripts': [
            'persistence=persistence:__main__',
        ]
    }
)
