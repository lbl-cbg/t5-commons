Overview
--------

.. contents::
   :depth:  3

What is LapinPy?
################

Lapinpy is a framework that sits on top of `CherryPy <http://www.cherrypy.org>`_ that focuses on delivering RESTFul webservices with minimal web content.
It allows users to focus the code to generate models and then use templates to render the content.
If you just want quickly create a few webpages and you don't care about providing RESTFul web services, than you probably shouldn't use lapinpy.

How does it work?
#################
LapinPy by itself is just a webpage with a default template that handles authentication via google oauth2. To add content you must create an "Application" which is a python file with a class that extends one of the Restful classes that comes with LapinPy. An application then has methods that has the method signature of (self, args, kwargs) and a name that start with:

    - get - a method that should only return data, it should not change data in the backend
    - put - a method that accepts data via kwargs and updates some data in the backend
    - post - a method that accepts data via kwargs and adds new data to the backend
    - delete - a method that will delete some data in the backend

URLs will automaticly be built based on the file name of the application and the method's name. An application with the file blah.py and a method of get_something(self, args, kwargs) will be accessible via the url /blah/something and any additional depth to the url will be stored in the args variable as a list and in the case of get or delete the query string will be passed as a dict to the kwargs variable. In the case of post or put the http payload will be deserialized and passed to the kwargs as a dict.
