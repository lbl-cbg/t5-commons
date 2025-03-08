import os
from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def listFiles(folder):
    return [os.path.join(folder, filename) for filename in os.listdir(os.path.join(os.path.dirname(__file__), folder))]


setup(
    name="jat",
    version="0.1.0",
    author=["Alexander Boyd", "Chris Beecroft", "Ed Lee"],
    author_email=["aeboyd@lbl.gov", "cjbeecroft@lbl.gov", "elee@lbl.gov"],
    description=("A system for managing collections of files in JAMO"),
    keywords="data metadata",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    entry_points={
        'console_scripts': [
            'jat-init=jat.init:main',
        ],
    },
    package_data={
        'jat': ['templates/*.html', 'doc/index.html', 'doc/overview/*.html'],
    },
    # data_files=data_files,
    long_description=read('README.md'),
    zip_safe=False,
    install_requires = [
        "pymongo==3.12.3",
        "pymysql==0.9.3",
    ],
)
