.. _tutorials:

Tutorials
---------


This tutorial will walk you through basic but complete LapinPy applications
that will show you common concepts as well as slightly more advanced ones.
We will assume you have a config file called my_config and it has the basics
to start lapind

.. contents::
   :depth:  4


Tutorial 1: Hello World
#######################

This is a very simple Application that doesn't connect to any 
database and just has one method.

.. code-block:: python
   :linenos:

    from lapinpy import restful

    class Hello(restful.Restful):
        
        @restful.generatedHtml
        def get_world(self, args, kwargs):
            return kwargs

Store this in the file hello.py and execute it with:

.. code-block:: bash

    $lapind my_config hello.py


Then go to the page http://localhost:8080/hello/world
you should see nothing, but then pass a query string to it like:

http://localhost:8080/hello/world?name=Kyle

you should now see name: Kyle on the page

or go to the page http://localhost:8080/api/hello/world?name=Kyle

You should see the json document:

.. code-block:: javascript

    {
        "name": "Kyle"
    }



Tutorial 2: Simple CRUD application
###################################

Here we will not use any database and instead just store all
the data in a hash. But we will be able to manipulate this data
with simple api calls

.. code-block:: python
    
    from lapinpy import restful
    import string, random
    
    class File(restful.Restful):
        
        def __init__(self, config):
            self.files = {}

        def post_file(self, args, kwargs):
            _id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(12))
            self.files[_id]=kwargs
            return {'id':_id}

        def get_file(self, args, kwargs):
            return self.files[args[0]]

        def put_file(self, args, kwargs):
            self.files[args[0]].update(kwargs)

        def delete_file(self, args, kwargs):
            del self.files[args[0]]


.. note:: There is no validation going on so I wouldn't use this in production.


Tutorial 3: CRUD with MySQL backend
###################################
Now lets use a mysql backend to get persistent data. This doesn't do any
validating so if a user puts in the wrong keyword mysql will complain and
the user will get an unfriendly error.

Also the database structure must already exist, this will not automatically
create the database structure.

.. code-block:: python

    from lapinpy import restful, mysqlrestful

    class File(mysqlrestful.MySQLRestful):
        def __init__(self, config):
            mysqlrestful.MySQLRestful.__init__(self, 'host', 'user', 'password', 'database')

        def get_file(self, args, kwargs):
            return self.query('select * from file where file_id=%s', args)

        def post_file(self, args, kwargs):
            return self.smart_insert('file', kwargs)

        def put_file(self, args, kwargs):
            return self.smart_modify('file', 'file_id=%d'%args[0], kwargs)

        def delete_file(self, args, kwargs):
            return self.delete('delete from file where file_id=%s'args[0])



Tutorial 4: CRUD with helper decorators
#######################################
Now lets use some of LapinPy's decorators to make a better application.
The use of @restful.validate is very important, this will ensure that
data types stay proper and extra data isn't passed in. It will also take
care of converting accidential string into ints or bools if the user provided
the incorrect type.

.. code-block:: python

    from lapinpy import restful, mysqlrestful

    class File(mysqlrestful.MySQLRestful):

        def __init__(self, config):
            mysqlrestful.MySQLRestful.__init__(self, 'host', 'user', 'password', 'database')

        @restful.generatedHtml(title='File {{file_id}}')
        @restful.single
        @restful.validate(argsValidator=[{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}])
        def get_file(self, args, kwargs):
            return self.query('select * from file where file_id=%s', args)

        @restful.validate({
                'file_path':{'type':str}
                'file_name':{'type':str}
                'file_size':{'type':int, 'required':False}
            }, allowExtra=False)
        def post_file(self, args, kwargs):
            return self.smart_insert('file', kwargs)

        @restful.validate({'file_size':{'type':str}}, [{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}], False)
        def put_file(self, args, kwargs):
            return self.smart_modify('file', 'file_id=%d'%args[0], kwargs)

        @restful.permission('delete')
        @restful.validate(argsValidator=[{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}])
        def delete_file(self, args, kwargs):
            return self.delete('delete from file where file_id=%s'args[0])
