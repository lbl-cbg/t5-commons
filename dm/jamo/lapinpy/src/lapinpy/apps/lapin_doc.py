from lapinpy import restful
import os
from lapinpy.common import HttpException
import cherrypy
from mimetypes import MimeTypes
mime = MimeTypes()


class Lapin(restful.Restful):

    def __init__(self, config=None):
        self.config = config
        self.mod_mappings = {}
        self.base = os.path.realpath(
            os.path.join(os.path.split(os.path.realpath(__file__))[0], '..', '..', 'docs', 'html'))

    auto_reload = True

    @restful.raw
    def get_docs(self, args, kwargs):
        if len(args) == 0:
            args = ['index.html']
        file = os.path.join(self.base, *args)
        if not os.path.realpath(file).startswith(self.base):
            raise HttpException(404, 'You have gone to an invalid page')
        cherrypy.response.headers['Content-Type'] = mime.guess_type(file)[0]
        with open(file) as f:
            return f.read()
