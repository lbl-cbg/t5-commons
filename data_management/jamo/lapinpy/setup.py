import os
from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="lapinpy",
    version="0.1.0",
    author="Alexander Boyd",
    author_email="aeboyd@lbl.gov",
    description=("A Restful framework that makes ui development quick and easy "),
    keywords="UI framework restful",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    entry_points={
        'console_scripts': [
            'lapind=lapinpy:main',
        ],
    },
    package_data={
        'lapinpy': ['db/tables.sql', 'db/migrate/*.sql', 'templates/*.html', 'scripts/*.js', 'scripts/*.html', 'scripts/*.css', 'images/*.png', 'images/*.jpg'],
    },
    long_description=read('README.md'),
    zip_safe=False,
    install_requires = [
        "python-daemon==2.2.4",
        "cherrypy==18.8.0",
        "pymongo==3.12.3",
        "pymysql==0.9.3",
        "six==1.16.0",
        "pyyaml==6.0",
        "requests==2.28.1",
        "setuptools==70.0.0",
        "psutil",
        "Jinja2==3.0.3",
        "google-auth-oauthlib==0.4.1",
        "authlib==0.15.5",
        "prometheus-client==0.14.1",
        "python-dateutil==2.8.2",
        "future==0.18.2",
    ],
)
