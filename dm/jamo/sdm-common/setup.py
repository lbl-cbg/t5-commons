import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='sdm-common',
    version='0.1.0',
    description='Shared libraries and binaries for jamo and jat',
    package_dir={'': 'lib/python'},
    packages=['.', 'jqueue'],
    entry_points={
        'console_scripts': [
            'jat=jat_cli:main',
            'jamo=jamo_cli:main',
            'jadmin=jadmin_cli:main',
            'qt=qt_cli:main',
        ],
    },
    long_description=read('README.md'),
    install_requires=[
        "pyparsing==3.1.2",
    ]
)
