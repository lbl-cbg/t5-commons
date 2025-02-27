Installation
------------

LapinPy is a pure python library, so installation is very simple.

.. contents::
   :depth:  4

Requirements
############

LapinPy only has a few mandatory requirement, and the others are helper packages.
  - `PyYAML <http://pyyaml.org/wiki/PyYAMLDocumentation>`_ - isn't needed for any core functionality, but is there to allow config files to be in yaml
  - `CherryPy <http://docs.cherrypy.org/en/latest/index.html>`_ - the only needed dependency to start using LapinPy, it provides the http server service
  - `Jinja2 <http://jinja.pocoo.org/docs/>`_ - used for html templates
  - `oauth2client <https://code.google.com/p/google-api-python-client/wiki/OAuth2Client>`_ - used if oauth is the selected authentication method
  - `pyOpenSSL <https://github.com/pyca/pyopenssl>`_ - used if you wish to open a https port
  - `MySQL-python <http://mysql-python.sourceforge.net/MySQLdb-1.2.2/>`_ - used if you wish to connect to a mysql server
  - `pymongo <http://api.mongodb.org/python/current/>`_ - used if you wish to connect to a mongodb server
  - `python-daemon <https://pypi.python.org/pypi/python-daemon/1.5.5>`_ - used if you run LapinPy in a daemon mode
  - `cx_oracle <http://cx-oracle.sourceforge.net/>`_ - used if you wish to connect to an oracle server

Supported python version
########################

CherryPy supports Python 2.3 through to 2.7. Support for python 3 can be added if there is a need for it

Installing
##########

LapinPy is not currently in any package managers so must install from the sources.

.. code-block:: bash
    
    $ git clone https://bitbucket.org/berkeleylab/jgi-lapinpy.git
    $ cd jgi-lapinpy
    $ python setup.py install

Running
#######

LapinPy comes with a binary called lapind which can be used to start up your instance.

.. code-block:: bash
    
    $lapind start <config>

config is the path to your :ref:`configuration <config>` file.
