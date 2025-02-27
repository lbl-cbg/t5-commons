Getting Started
---------------

.. contents::
   :depth:  4


Configuration File
##################
LapinPy needs to know a few things before it can start, so we need to create a configuration file(s). 
LapinPy uses a custom configuration manager that allows for on the fly config changes without having to restart the instance and the ability to have "Application" specific settings that are hidden from other apps. All settings are stored in yaml files with the extension ".config" that are put in a folder of your choice.

In order to start LapinPy you need to have a lapinpy.config file with a few required values: 
    - port: what port to run on
    - url: this will over ride the generated url, useful if there is port forwarding going on
    - site_name: the string that will appear at the top of the site
    - core_db: the name of the database to store the core info in
    
All other optional configuration settings are: 
    - hostname: the name of the host that the instance is running on, this is really the url minus the port and the http/s, use this if you want to force connections to listen only on a specific interface
    - ssl_port: what port to run ssl on, if used the ssl_* keys are required
    - ssl_cert: the path to the file that contains the cert
    - ssl_private_key: the path to the file that contains the private key
    - ssl_certificate_chain: the path to the file that contains the chain
    - instance_type: default dev, should be used with the decorator @usewhen
    - email_to: what email address to send all critical errors to
    - enable_cron: if not true cron events will not trigger, default is true
    - oauthsecretfile: the full path to the location of the oauth secrets file

A basic example to get LapinPy started

.. code-block:: yaml

    port:  8080
    url : localhost:8080
    site_name : My test site
    core_db : test.db

Save that to my_config_folder/lapinpy.config and then run the following:

.. code-block:: bash

    $lapind my_config_folder

You should now be able to go to the site http://localhost:8080

Application Settings
####################
Each application can have its own settings that can't been seen by other applications. To create a settings file for the application inside file hello.py you 
must name the settings file hello.config. In there you can put any key-value in yaml format and you can access it with the config object that is passed to the 
application constructor. You can also override global shared settings in here as LapinPy resolves these settings after the ones contained in lapinpy.config ex:

hello.config

.. code-block:: yaml

    mysql_user: 'test'
    mysql_host: 'localhost'
    mysql_pass: 'pass'
    database: 'blah'

hello.py

.. code-block:: python

   class Hello(mysqlrestful.MySQLRestful):
       def __init__(self, config):
           mysqlrestful.MySQLRestful.__init__(self, config.mysql_host, config.mysql_user, config.mysql_pass, config.database)


Shared Settings
###############
Inside the lapinpy.config you can have shared settings that all apps can see, just put these settings in the shared dictonary. Anything in here can get overwritten
by the application's settings file if it exists. Ex:

lapinpy.config

.. code-block:: yaml

    port: 8080
    shared:
      dw_address: 'http://something.com'



All configuration keys are passed to each Application and can be accessed by going self.config.key . 
This is useful if you don't want to store passwords in the source code.

Now you can go to the Tutorials to see for examples to create your first page!
