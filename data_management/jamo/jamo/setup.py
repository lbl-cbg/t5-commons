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
    name="jamo",
    version="0.1.0",
    author=["Alexander Boyd", "Chris Beecroft", "Ed Lee"],
    author_email=["aeboyd@lbl.gov", "cjbeecroft@lbl.gov", "elee@lbl.gov"],
    description=("A system for managing data with rich metadata records and automated transfer and backups"),
    keywords="data metadata",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    entry_points={
        'console_scripts': [
            'jamo-init=jamo.init:main',
            'dt-service=jamo.dt_service:main',
            'egress-handler=jamo.egress_handler:main',
            'globus-cleanup=jamo.globus_cleanup:main',
        ],
    },
    package_data={
        'jamo': ['templates/*.html', 'doc/*'],
    },
    # data_files=data_files,
    long_description=read('README.md'),
    zip_safe=False,
    install_requires = [
        "matplotlib==3.9.0",
        "pymongo==3.12.3",
        "pymysql==0.9.3",
        "pyyaml==6.0",
    ],
)
