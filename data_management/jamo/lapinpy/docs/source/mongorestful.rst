MongoRestful.py
_______________

.. toctree::
   :maxdepth: 3


Public Methods
##############

.. autoclass:: lapinpy.mongorestful.MongoRestful
   :members:


Example
#######
A simple CRUD application that stores data in mysql

.. code-block:: python
   :linenos:
    
    from lapinpy import restful, mongorestful
   
    class File(mongorestful.MongoRestful):
        def __init__(self):
            mongorestful.MongoRestful.__init__(self, 'host', 'user', 'password', 'database', 'options')

        @restful.generatedHtml(title='File {{file_id}}')
        @restful.single
        @restful.validate(argsValidator=[{'name':'file_id', 'type':'oid', 'doc':'The file id of the file to get'}])
        def get_file(self, args, kwargs):
            return self.findOne('file', file_id=args[0])
      
        @restful.validate({
                'file_path':{'type':str}
                'file_name':{'type':str}
                'file_size':{'type':int, 'required':False}
            }, allowExtra=False)
        def post_file(self, args, kwargs):
            return self.save('file, kwargs)

        @restful.validate({'file_size':{'type':str}}, [{'name':'file_id', 'type':'oid', 'doc':'The file id of the file to get'}], False)
        def put_file(self, args, kwargs):
            return self.update('file', {'file_id':args[0]}, kwargs)

        @restful.permission('delete')
        @restful.validate(argsValidator=[{'name':'file_id', 'type':'oid', 'doc':'The file id of the file to get'}])
        def delete_file(self, args, kwargs):
            return self.remove('file', {'file_id': args[0]} )
