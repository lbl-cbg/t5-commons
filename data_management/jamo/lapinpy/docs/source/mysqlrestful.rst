MySQLRestful.py
_______________

.. toctree::
   :maxdepth: 3


Public Methods
##############

.. autoclass:: lapinpy.mysqlrestful.MySQLRestful
   :members:


Example
#######
A simple CRUD application that stores data in mysql

.. code-block:: python
    
    from lapinpy import restful, mysqlrestful
   
    class File(mysqlrestful.MySQLRestful):
        def __init__(self):
            mysqlrestful.MySQLRestful.__init__(self, 'host', 'user', 'password', 'database')

        @restful.generatedHtml(title='File {{file_id}}')
        @restful.single
        @restful.validate(argsValidator=[{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}])
        def get_file(self, args, kwargs):
            return self.query('select * from file where file_id=%s, args)
      
        @restful.validate({
                'file_path':{'type':str}
                'file_name':{'type':str}
                'file_size':{'type':int, 'required':False}
            }, allowExtra=False)
        def post_file(self, args, kwargs):
            return self.smart_insert('file, kwargs)

        @restful.validate({'file_size':{'type':str}}, [{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}], False)
        def put_file(self, args, kwargs):
            return self.smart_modify('file', 'file_id=%d'%args[0], kwargs)

        @restful.permission('delete')
        @restful.validate(argsValidator=[{'name':'file_id', 'type':int, 'doc':'The file id of the file to get'}])
        def delete_file(self, args, kwargs):
            return self.delete('delete from file where file_id=%s'args[0])
