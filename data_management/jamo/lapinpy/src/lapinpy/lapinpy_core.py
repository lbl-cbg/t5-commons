### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from calendar import monthrange
from cherrypy.lib.static import serve_file
from .config_util import ConfigManager
from .curl import Curl
from decimal import Decimal
from jinja2 import Environment, FileSystemLoader
from .job import Job
from google_auth_oauthlib.flow import Flow
from .jqueue.queuemanager import QueueManager
from signal import signal, SIGINT
from .singleton import Singleton
from collections import OrderedDict
from prometheus_client import Histogram, Counter, start_http_server
import base64
import cherrypy
from . import common
import copy
from . import sdmlogger
import datetime
import imp
import json
import os.path
import random
import re
import string
import sys
import types
import logging
import time
import hashlib
import shutil
from urllib.parse import urlparse, parse_qs, urlencode
# from urlparse import urlparse, parse_qs
# from urllib import urlencode

from pkg_resources import resource_filename

import ssl
from functools import wraps

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import long
from past.builtins import basestring
from future.utils import iteritems
from future import standard_library
standard_library.install_aliases()
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


def sslwrap(func):
    @wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLS
        return func(*args, **kw)

    return bar


ssl.wrap_socket = sslwrap(ssl.wrap_socket)
cipher = None


class AppImportFinder():

    def __init__(self, folder, appname):
        self.folder = folder
        self.files = []
        self.loaded_files = {}
        self.logger = sdmlogger.getLogger('importer')
        self.logger.setEntities(app=appname)
        for root, folders, files in os.walk(folder):
            rel_folder = root.replace(folder, '')
            if rel_folder.startswith('/'):
                rel_folder = rel_folder[1:]
            for file in files:
                if file.endswith('.py'):
                    self.files.append('.'.join(os.path.join(rel_folder, file[:-3]).split('/')))
            for file in folders:
                self.files.append('.'.join(os.path.join(rel_folder, file).split('/')))

    def find_module(self, fullname, path=None):
        if fullname in self.files:
            return self

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        fullpath = os.path.join(self.folder, '/'.join(fullname.split('.')))
        mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
        mod.__loader__ = self
        if os.path.isdir(fullpath):
            mod.__package__ = fullname
            mod.__path__ = [fullpath]
            mod.__file__ = fullpath
        else:
            mod.__package__ = fullname.rpartition('.')[0]
            mod = imp.load_source(fullname, fullpath + '.py')
            self.loaded_files[fullname] = fullpath + '.py'
            self.logger.info('loaded %s from %s', fullname, fullpath + '.py')
        mod.__appimport__ = True
        return mod


def encrypt(string):
    if len(string) % 16 != 0:
        # TODO: The cipher does not seem to support unicode strings and we're making the string unicode
        string += (u'\x00' * (16 - (len(string) % 16)))
    return base64.b64encode(cipher.encrypt(string))


def decrypt(string):
    # TODO: Cannot replace bytes with a str in Py3. Although we may not want to remove the padding since cipher does not
    #  support unicode strings
    return cipher.decrypt(base64.b64decode(string)).decode('utf-8').replace(u'\x00', '')


def link(address, title):
    return '<a href="%s">%s</a>' % (address, title)


def jsonify(obj):
    if isinstance(obj, type):
        return obj.__name__
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return '%.2f' % obj
    elif hasattr(obj, '__str__'):
        return str(obj)
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))


def to_json(data):
    return json.dumps(data, default=jsonify)


class PageResponse(list):
    def __init__(self, page1, module):
        self.dic = page1
        self.module = module
        self.on = -1

    def __len__(self):
        return self.dic['record_count']

    def __iter__(self):
        class Iterat():
            def __init__(iself):
                iself.i = -1
                iself.si = -1

            def __iter__(iself):
                return iself

            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            def next(self):
                return self.__next__()
            ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

            def __next__(iself):
                if iself.i < self.dic['record_count'] - 1:
                    iself.i += 1
                    if iself.si + 2 > len(self.dic['records']):
                        iself.si = -1
                        self.dic['records'] = \
                            RestServer.Instance().run_method(self.module, 'get_nextpage', self.dic['cursor_id'])['records']
                    iself.si += 1
                    return self.dic['records'][iself.si]
                raise StopIteration

        return Iterat()


@Singleton
class RestServer:
    apps = {}
    flow = None
    rest_ui_components = {}
    rootdir = resource_filename(__package__, '')
    env = Environment(loader=FileSystemLoader(resource_filename('lapinpy', 'templates')))
    env.filters['jsonify'] = to_json
    config = None
    request_metrics_duration = None
    request_metrics_serialization_duration = None
    request_metrics_size = None
    request_metrics_error = None

    def __init__(self):
        self.logger = sdmlogger.getLogger('LapinPy')
        self.cachedMenus = {}
        self.search_components = {}
        self.currentImporter = None
        self.loadCallbacks = {}
        # Temporary fix for mongorestful services being called from here
        self.temp_mongo = None
        self.request_metrics_duration = Histogram('lapinpy_request_duration_seconds', 'request duration in seconds', ['method', 'endpoint', 'module', 'source_ip'])
        self.request_metrics_serialization_duration = Histogram('lapinpy_request_serialization_duration_seconds', 'request serialization duration in seconds', ['method', 'endpoint', 'module', 'source_ip'])
        self.request_metrics_size = Histogram('lapinpy_request_reponse_size', 'request size in bytes', ['method', 'endpoint', 'module', 'source_ip'], buckets=[int(10**x) for x in range(10)])
        self.request_metrics_errors = Counter('lapinpy_request_errors', 'number of not successful endpoint requests', ['method', 'endpoint', 'module', 'kind', 'source_ip'])

    userdata = {'asdf': 'asdf'}

    # replace with str.isidentifier() when we move to python 3
    def isidentifier(self, text):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: This is not technically correct as `123` is a valid identifier.
        return re.match(r'^[a-zA-Z0-9_]+$', text)
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup
        # return text.isidentifier()
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

    def getcol(self, id, map=None, value=None):
        if map is None:
            type = 'string'
            if value is not None:
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                if (isinstance(value, str) and value.isdigit()) or isinstance(value, (float, int, long)):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - remove this noqa comment after migration cleanup
                # if (isinstance(value, str) and value.isdigit()) or isinstance(value, (float, int)):  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    type = 'number'
            return {'id': id, 'label': id.replace('_', ' ').title(), 'type': type, 'pattern': ''}
        title = map['title'] if 'title' in map else id.replace('_', ' ').title()
        type = map['type'] if 'type' in map else 'string'
        if type == 'html':
            type = 'string'
        return {'id': id, 'label': title, 'type': type, 'pattern': ''}

    def error(self, errorcode, messages):
        cherrypy.response.status = errorcode
        error = {}
        error['error_code'] = errorcode
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(messages, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(messages, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            error['errors'] = [messages]
        else:
            error['errors'] = messages
        return error

    def check_permissions(self, method_ref=None, permissions=[], method_perms=None):
        if permissions is None:
            permissions = []
        if method_perms is None:
            if hasattr(method_ref, 'permissions'):
                method_perms = method_ref.permissions
        if method_perms is not None:
            for perm in method_perms:
                if perm not in permissions:
                    return False
        return True

    @cherrypy.expose
    def default(self, *args, **kwargs):
        response = None
        permissions = None
        cookies = cherrypy.request.cookie
        user_info = None

        # first thing to do is determine credentials
        if 'Authorization' in cherrypy.request.headers:
            auth = cherrypy.request.headers['Authorization']
            auth = auth.split(' ')
            if len(auth) > 1:
                token = auth[1]
                if auth[0] == 'Bearer':
                    permissions = self.core.get_permissions_from_user_token(token)
                    user_info = self.core.get_userinfo_from_user_token(token)
                elif auth[0] == 'Application':
                    permissions = self.core.get_apppermissions((token), None)
                    user_info = self.core.get_appinfo_from_token(token)
        elif 'sessionid' in cookies:
            userid = cookies['sessionid'].value
            permissions = self.core.get_permissions_from_user_token(userid)
            user_info = self.core.get_userinfo_from_user_token(userid)

        if len(args) > 0 and args[0] == 'api':
            endpoint = '/' + '/'.join(args[0:3])
            module = args[1]
            if 'Content-Length' in cherrypy.request.headers:
                # TODO: Hack to get body JSON and form requests to work when `Content-Type` is
                #  `application/x-www-form-urlencoded'`. Remove this once we disable the hack...
                # BEGIN_HACK
                if hasattr(cherrypy.request.body, 'rawbody'):
                    rawbody = cherrypy.request.body.rawbody
                else:
                # END_HACK # noqa: E115
                    try:
                        # read will only work with a put, not a get (nor delete)
                        rawbody = cherrypy.request.body.read()
                    except Exception:
                        # we are here likely because of a delete
                        rawbody = cherrypy.request.body.readline()
                if rawbody in (None, ''):
                    for kwarg in kwargs:
                        # TODO: Is this a bug? Should we be assigning `rawbody` the name of the key, not the value?
                        rawbody = kwarg
                        break
                if isinstance(rawbody, bytes):
                    rawbody = rawbody.decode('utf-8')
                if rawbody not in (None, '', 'XXredirect_internalXX'):
                    try:
                        kwargs = json.loads(rawbody)
                    except Exception:
                        # Not necessarily and error, stop logging this
                        # self.logger.warning('failed to parse raw body %s', rawbody)
                        if '{' in rawbody:
                            response = self.error(422, 'The request body is malformed or invalid JSON: %s' % rawbody)
                        pass
            # attempt to do the restful commands

            method_ref = None
            if len(args) == 1:
                response = self.error(400,
                                      'The request url is invalid. You must request a module to call. Supported modules are: %s' % (
                                          ','.join(self.apps)))
            elif len(args) == 2:
                response = self.error(400, 'The request url is invalid. You must request a resource')
            elif not response:
                parms = []
                for arg in args[3:]:
                    parms.append(arg)
                module = args[1]
                cherrypy.response.headers['Content-Type'] = 'application/json; charset=utf-8'
                cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
                cherrypy.response.headers['Access-Control-Allow-Methods'] = 'GET, PUT, POST, DELETE, OPTIONS'
                cherrypy.response.headers['Access-Control-Max-Age'] = '1000'
                cherrypy.response.headers[
                    'Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
                if cherrypy.request.method == 'OPTIONS':
                    return None
                method = cherrypy.request.method.lower() + '_' + args[2]
                if module not in self.apps:
                    response = self.error(400,
                                          'The module you have requested: %s is not a valid module on this system. Supported modules are : %s ' % (
                                              module, ','.join(self.apps)))
                    self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='invalid_module').inc()
                else:
                    try:
                        if hasattr(self.apps[module], method):
                            methodcall, pop_off = self.apps[module].getrestmethod(cherrypy.request.method.lower(), args[2:])
                            parms = parms[pop_off:]
                            method_ref = methodcall
                            if hasattr(methodcall, 'sort') and methodcall.sort is not None:
                                kwargs['sort'] = methodcall.sort
                            if self.check_permissions(methodcall, permissions):
                                if hasattr(methodcall, 'passreq'):
                                    if user_info is None:
                                        raise common.HttpException(401,
                                                                   'You must authenticate your self to access this page: method %s in module %s' % (
                                                                       method, module))
                                    if methodcall.joinauth is True:
                                        if 'group' not in user_info or user_info['group'] is None:
                                            raise common.HttpException(401,
                                                                       'You must set your primary group before you can use this api')
                                        kwargs.update(user_info)
                                    else:
                                        kwargs['__auth'] = user_info
                                    if methodcall.include_perms:
                                        kwargs['permissions'] = permissions

                                # per endpoint
                                #   - throughput, rate, duration, quantiles
                                #   request_duration{endpoint=/api/query, method=post, quantile=95} 10
                                start_measure = time.time()
                                response = methodcall(parms, kwargs)
                                end_measure = time.time()
                                self.request_metrics_duration.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip).observe(end_measure - start_measure)
                                if hasattr(methodcall, 'raw'):
                                    return response
                            elif permissions is not None:
                                response = self.error(403,
                                                      'You are not authorized to access the method you have requested: method %s in module %s' % (
                                                          method, module))
                                self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='unauthorized').inc()
                            else:
                                response = self.error(401,
                                                      'You must authenticate your self to access this page: method %s in module %s' % (
                                                          method, module))
                                self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='unauthenticated').inc()
                        else:
                            response = self.error(400,
                                                  'Sorry module %s does not have the method %s that you have requested' % (
                                                      module, method))
                            self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='invalid_method').inc()
                    except common.ValidationError as e:
                        self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='validation').inc()
                        response = self.error(400, e.error)
                    except common.HttpException as e:
                        self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='http').inc()
                        response = self.error(e.code, e.message)
                    except Exception as e:
                        self.logger.critical('Unknown exception occurred while calling %s %s', module, method)
                        self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='unexpected').inc()
                        response = self.error(500, 'Sorry the following exception was caught: %s' % e)

            if kwargs is not None and 'tq' in kwargs:
                return self.googleChartify(response, method_ref, args, kwargs)

            if 'XXredirect_internalXX' in kwargs:
                raise cherrypy.HTTPRedirect(kwargs['XXredirect_internalXX'].replace('/api', ''))
            try:
                start_measure = time.time()
                response = json.dumps(response, default=jsonify)
                end_measure = time.time()
                self.request_metrics_serialization_duration.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip).observe(end_measure - start_measure)
                self.request_metrics_size.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip).observe(len(response))
                return response
            except Exception:
                self.request_metrics_errors.labels(method=cherrypy.request.method, endpoint=endpoint, module=module, source_ip=cherrypy.request.remote.ip, kind='render_json').inc()
                raise

        if len(args) > 2 and args[1] in ('scripts', 'images'):
            app = args[0]
            if app not in self.apps:
                return self.error(404, 'Sorry the path at %s does not exist' % app)
            return serve_file(os.path.join(self.apps[app].location, *args[1:]))
        if 'sessionid' not in cookies:
            cookies = cherrypy.response.cookie
            cookies['sessionid'] = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
        userid = cookies['sessionid'].value
        usersection = "<a href='{}'>Login</a>".format(
            self.get_authorization_url(args)) if self.flow is not None else ''

        if userid in self.userdata:
            usersection = "<div id='username'>Welcome %s</div>" % self.userdata[userid]['name']
        else:
            userdata = self.core.get_user_from_id(userid)
            if len(userdata) > 0:
                self.userdata[userid] = userdata[0]
                usersection = "<div id='username'>Welcome %s</div>" % self.userdata[userid]['name']

        menus = self.getMenus(permissions, user_info)

        if len(args) > 0 and args[0] == 'globalsearch':
            tmpl = self.env.get_template('template.html')
            # Address potential x-site scripting
            what = ''.join(filter(self.isidentifier, kwargs.get('what', None)))
            query = ''.join(filter(self.isidentifier, kwargs.get('query', None)))
            if what == 'global':
                pagedetails = {'title': 'Search results for: %s' % query, 'additional_content': []}
                for app in self.apps:
                    if hasattr(self.apps[app], 'search'):
                        results = self.apps[app].search(query)
                        if len(results) > 0:
                            pagedetails['additional_content'].append({'label': app, 'content': '<br>'.join(results)})
            else:
                url = self.search_components[what + '/' + query]
                raise cherrypy.HTTPRedirect(url)
            return tmpl.render(searchOptions=self.search_components, menus=menus, username=usersection, logo=self.config.logo, favicon=self.config.favicon,
                               pagedetails=pagedetails, site_title=self.config.site_name, perms=permissions)

        elif len(args) > 0 and args[0] == 'oauth2callback':
            self.__oauth2_callback(userid, kwargs)
        else:
            tmpl = self.env.get_template('template.html')
            try:
                pagedetails = self.getPageDetails(args, kwargs, permissions, user_info)
            except Exception as e:
                cherrypy.response.status = 500
                self.logger.error(e)
                pagedetails = {'error_code': 500, 'errors': e}
            details = tmpl.render(searchOptions=self.search_components, menus=menus, username=usersection, logo=self.config.logo, favicon=self.config.favicon,
                                  pagedetails=pagedetails, site_title=self.config.site_name, perms=permissions)
            return details

    def __oauth2_callback(self, userid, kwargs):
        """Callback for oauth2 authorization call. Fetches the credentials token and writes user information to the
        database (if it doesn't already exist) and then associates the user to the token in the database. It then
        redirects the user to the relative path of `state` if available, otherwise root path.

        :param str userid: User id
        :param dict[str, str] kwargs: Dictionary should contain values for the following keys:
            ['code']: Authorization code to retrieve credentials (required)
            ['state']: Relative path from page where login flow was initiated (optional)
        :raises cherrypy.HTTPRedirect: Exception to initiate page redirection
        """
        try:
            self.flow.fetch_token(code=kwargs.get('code'))
            credentials = self.flow.credentials
            curl = Curl('https://www.googleapis.com')
            curldata = curl.get('oauth2/v1/userinfo', {'access_token': credentials.token})
            # now add this info into the database
            id = self.core.get_user([curldata.get('email')], None)
            if id is None:
                id = self.core._post_user(None, curldata)
            self.core.associate_user_token(id.get('user_id'), userid)
            self.userdata[userid] = curldata
            redirect = kwargs.get('state') if 'state' in kwargs else '/'
            raise cherrypy.HTTPRedirect(redirect)
        except ValueError as e:
            sdmlogger.getLogger('restserver').error('credentials retrieval error happened: ' + repr(e))
            raise cherrypy.HTTPRedirect('/')

    def get_authorization_url(self, args):
        """Get the authorization URL for Google OAuth2.

        :param list[str] args: Current relative path parts
        :return: str for the authorization URL
        """
        # JAMO will only send strings and numbers for page redirects.  If there is anything else there, ignore this
        # param as it could be a cross-scripting issue.
        url_start_args = '/'.join(args) if self.isidentifier(''.join(args)) else ''
        url_parts = urlparse(self.flow.authorization_url()[0])
        url_query_parameters = parse_qs(url_parts.query)
        # The OAuth library sets the state query parameter with a random string, we override the value to store the
        # current relative path when triggering the OAuth flow so that the user can be redirected to the appropriate
        # page after the flow is completed.
        url_query_parameters['state'] = url_start_args
        updated_url_query_parameters = urlencode(url_query_parameters, doseq=True)
        return url_parts._replace(query=updated_url_query_parameters).geturl()

    def getMenus(self, permissions, user_info):
        # If we've not logged in, just return the home page
        if not user_info:
            return [{'href': '/', 'name': 'home', 'order': 0}]
        if permissions is None:
            permissionStr = ''
        else:
            permissionStr = ','.join(permissions)
        if permissionStr in self.cachedMenus:
            return self.cachedMenus[permissionStr]
        ret = {'pages': {'home': {'href': '/', 'order': 0}}}
        for appName in self.apps:
            app = self.apps[appName]
            menus = app.menus
            for menu in menus:
                if self.check_permissions(permissions=permissions, method_perms=menu['permissions']):
                    locs = (app.menuname + '>' + menu['title']).split('>')
                    current = ret
                    for loc in locs:
                        if 'pages' not in current:
                            current['pages'] = {}
                        if loc not in current['pages']:
                            current['pages'][loc] = {}
                        current = current['pages'][loc]
                    current['href'] = menu['href']
            if len(menus) > 0 and app.menuname.split('>')[0] in ret['pages']:
                ret['pages'][app.menuname.split('>')[0]]['order'] = app.order

        fret = []
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for page, value in iteritems(ret['pages']):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for page, value in ret['pages'].items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            # Sort the sub-menu items.  Not the right place to do this, should be a function call too.
            if 'pages' in value:
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                for spage, svalue in iteritems(value['pages']):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # for spage, svalue in value['pages'].items():  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    if 'pages' in svalue:
                        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                        svalue['pages'] = OrderedDict(sorted(iteritems(svalue['pages'])))
                value['pages'] = OrderedDict(sorted(iteritems(value['pages'])))
                        ### PYTHON2_END ###  # noqa: E266,E116 - to be removed after migration cleanup
                        ### PYTHON3_BEGIN ###  # noqa: E266,E116 - to be removed after migration cleanup
                        # TODO: uncomment code below during cleanup  # noqa: E116 - to be removed after migration cleanup
                        # svalue['pages'] = OrderedDict(sorted(svalue['pages'].items()))  # noqa: E116 - remove this noqa comment after migration cleanup
                # value['pages'] = OrderedDict(sorted(value['pages'].items()))
                ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
            npage = value
            value['name'] = page
            if value['order'] is not None and len(fret) > value['order']:
                fret.insert(value['order'], npage)
            else:
                fret.append(npage)
        self.cachedMenus[permissionStr] = fret
        return fret

    def createJsonTestObj(self, validator):
        if 'type' in validator and isinstance(validator['type'], (tuple, type, list)):
            obj_type = validator['type']
            if isinstance(obj_type, (list, tuple)):
                obj_type = obj_type[0]

            if obj_type is list:
                if 'validator' in validator:
                    return [self.createJsonTestObj(validator['validator'])]
                return ['any_thing']
            elif obj_type is dict:
                if 'validator' in validator:
                    return self.createJsonTestObj(validator['validator'])
                return {'any_key': 'any value', 'any_type': 12345}
            elif obj_type is int:
                return random.randrange(0, 100)
            elif obj_type is str:
                return 'a string'
            elif obj_type is float:
                return 3.14159265359
            elif obj_type is bool:
                return False
            else:
                self.logger.error('Found object type that couldn\'t be decoded %s' % obj_type)
        else:
            if isinstance(validator, dict):
                ret = {}
                for key in validator:
                    ret[key] = self.createJsonTestObj(validator[key])
            else:
                return 'error'
            return ret

    error_validation = {'error_code': {'type': int}, 'errors': {'type': list, 'validator': {'type': str}}}

    def getJSONHtml(self, data, **kwargs):
        data = self.createJsonTestObj(data)
        for key in kwargs:
            data[key] = kwargs[key]
        return '<div class="json">' + json.dumps(data, default=jsonify, sort_keys=True, indent=4,
                                                 separators=(',', ':')).replace('\n', "<br>").replace(' ',
                                                                                                      '&nbsp') + '</div>'

    def createArgsPath(self, validators):
        path = ''
        for validator in validators:
            path += '/{%s}' % validator['name']
        return path

    def colapseData(self, results, method, additional_content, lkey=''):
        new_results = {}
        for key, value in results.items():
            if isinstance(value, list):
                key = str(key)
                temp_method = lambda: None  # noqa: E731
                temp_method.__name__ = str(lkey + key).replace('.', '_')
                temp_method.title = lkey + key
                if hasattr(method, 'map') and method.map is not None and lkey + key in method.map:
                    temp_method.map = method.map[lkey + key]
                    temp_method.onlyshowmap = True

                if len(value) and isinstance(value[0], dict):
                    additional_content.append(self.renderTable(temp_method, value))
                else:
                    li = ''
                    for val in value:
                        li += '<li>{}</li>'.format(val)
                    additional_content.append(
                        '<h3>{}</h3><ul class="colapse" id="{}">{}</ul>'.format(temp_method.title, temp_method.__name__,
                                                                                li))
            elif isinstance(value, dict):
                sresults = self.colapseData(value, method, additional_content, key + '.')
                for skey, svalue in sresults.items():
                    new_results['%s%s' % (lkey, skey)] = svalue
            else:
                new_results[lkey + key if lkey != '' else key] = value
        return new_results

    def getPageDetails(self, args, kwargs, permissions=None, user_info=None):
        # Address potential x-site scripting
        url_path = '/' + '/'.join(filter(self.isidentifier, args[:2]))

        type = None
        ret = {'title': None, 'rest_url': None, 'type': None}
        try:
            method = None
            ret['rest_url'] = '/api%s' % url_path
            if url_path in self.rest_ui_components:
                method = self.rest_ui_components[url_path]['method']
                ret['rest_url'] = '/api' + self.rest_ui_components[url_path]['rest_url']
            elif len(args) > 0 and args[0] in self.apps:
                method, pop_left = self.apps[args[0]].getrestmethod('get', args[1:])
                if method is None or not hasattr(method, 'is_ui') or not method.is_ui:
                    return self.error(404, 'The page you have requested: %s does not exist' % url_path)
            else:
                return self.error(404, 'The page you have requested: %s does not exist' % url_path)

            if not self.check_permissions(method, permissions):
                return self.error(403,
                                  'You are not authorized to access the requested page. If you need access please contact the admin of this site to request access')
            if 'passreq' in dir(method) and user_info is None:
                return self.error(401, 'You must be logged in to view this page')
            type = method.display_type
            method_props = dir(method)
            if hasattr(method, 'passreq'):
                if method.joinauth is True:
                    kwargs.update(user_info)
                else:
                    kwargs['__auth'] = user_info
            if hasattr(method, 'passuser'):
                kwargs['__user'] = user_info
            ret['title'] = method.title
            ret['type'] = type
            results = None

            if len(args) > 2:
                ret['rest_url'] = ret['rest_url'] + '/' + '/'.join(args[2:])
                if ret['title'] is not None:
                    ret['title'] = ret['title'].replace('{{value}}', args[2])

            if ret['title'] is not None and ret['title'].count('{{') > 0:
                field = ret['title'].split('{{')[1].split('}}')[0]
                results = common.customtransform(method(args[2:], kwargs))
                if field in results:
                    ret['title'] = ret['title'].replace('{{%s}}' % field, str(results[field]))

            if type == 'chart' and '/api' + url_path in self.core.chart_mappings:
                js = self.core.chart_mappings['/api' + url_path]
                js = re.sub(r'/api/.*tq=di', ret['rest_url'] + '?tq=di', js)
                ret['chart_details'] = js

            if type == 'queryResults':
                tables = []
                for qr in getattr(method, 'queryResults'):
                    qr['table']['title'] = ''
                    tables.append(self.renderTable(method, None, args[2:], qr, kwargs))
                ret['additional_content'] = tables
            elif type == 'generated':
                if results is None:
                    results = method(args[2:], kwargs)

                if 'is_single' in method_props or isinstance(results, (dict, common.CustomDict)):
                    ret['rest_url'] = None
                    additional_content = []

                    if isinstance(results, common.CustomDict):
                        results = results.dic

                    if 'links' in method_props:
                        for link in method.links:
                            function = link['function']
                            key = link['key']
                            data = results[key]
                            del results[key]
                            additional_content.append(self.renderTable(function, data, args))

                    if 'ui_links' in method_props:
                        for link in method.ui_links:
                            if hasattr(link, 'produce'):
                                if self.check_permissions(link.method, permissions):
                                    additional_content.insert(0, {'class': 'ui_link', 'content': link.produce(results, '/' + ('/'.join(args)))})
                            elif self.check_permissions(link, permissions):
                                additional_content.insert(0, {'class': 'ui_link', 'content': link(method.__self__, results)})

                    if 'table_links' in method_props:
                        for table in method.table_links:
                            api_key = table['api_key']
                            data = results.get(api_key, None)
                            del results[api_key]
                            additional_content.append(self.renderTable(method, data,
                                                                       [args[0], args[1], str(results.get(table.get('key'), args[2]))],
                                                                       table['queryResults'], kwargs))

                    additional_content.insert(0, self.renderTable(method,
                                                                  self.colapseData(results, method, additional_content),
                                                                  args))

                    ret['additional_content'] = additional_content
                elif isinstance(results, (list, common.CustomList)):
                    ret['additional_content'] = [self.renderTable(method, results, args)]
            else:
                kwargs['__ui'] = True
                if len(args) == 2:
                    results = method(args, kwargs)
                else:
                    results = method(args[2:], kwargs)

                # elif type is 'template':
                if type == 'template':
                    # kwargs['__ui'] = True
                    # results = method(args[2:], kwargs)
                    del ret['rest_url']

                    # If a template wasn't set in the setter, but returned, use that
                    if method.template is None and 'template' in results:
                        template = results['template']
                    else:
                        template = method.template

                    method_has_env = hasattr(method, 'env')
                    if hasattr(method, 'common_html') or not method_has_env:
                        tmpl = self.env.get_template(template)
                    elif method_has_env:
                        tmpl = method.env.get_template(template)
                    else:
                        tmpl = self.env.get_template(template)

                    if results is not None:
                        func_prop = {}
                        for prop in dir(method):
                            if not (prop.startswith('__') or prop.startswith('im')):
                                func_prop[prop] = getattr(method, prop)
                        response = tmpl.render(data=results, func_props=func_prop, args=args[2:], kwargs=kwargs)
                    else:
                        response = tmpl.render()

                    ret['additional_content'] = [response]
                elif type == 'form':
                    ret['rest_url'] = None
                    # results = method(args[2:], kwargs)
                    ret['additional_content'] = [self.createForm(method, results, '/' + ('/'.join(args)))]
                elif type == 'raw':
                    # results = method(args[2:], kwargs)
                    del ret['rest_url']
                    ret['additional_content'] = [str(results)]

            if hasattr(method, 'custom_template'):
                ret['additional_content'].append(method.env.get_template(method.custom_template).render())

            return ret
        except common.HttpException as e:
            return self.error(e.code, e.message)

    def createForm(self, method, results, currentPage=None):
        map = method.map
        action = method.submitto
        if action.count('{{') > 0:
            field = action.split('{{')[1].split('}}')[0]
            if field in results:
                action = action.replace('{{%s}}' % field, str(results[field]))

        ret = '<form class="form" method="' + method.method + '" action="' + action + '"><table><tr>'
        for keyMap in map:
            key = keyMap['key']
            inputType = keyMap['type']
            title = key.replace('_', ' ').title() if 'title' not in keyMap else keyMap['title']
            value = str(results[key]) if key in results and results[key] is not None else ''
            ret += '<td class="form_title">%s:</td><td>' % title
            if inputType in ('number', 'string'):
                ret += '<input name="%s" value="%s">' % (key, value)
            elif inputType == 'bigstring':
                ret += '<textarea name="%s">%s</textarea>' % (key, value)
            elif inputType == 'selection':
                ret += '<select name="%s">' % key
                options = keyMap['options']
                for option in options:
                    name = option if isinstance(options, list) else options[option]
                    ret += '<option ' + ('selected ' if option == value else '') + ' value="%s">%s</option>' % (option, name)
                ret += '</select>'
            ret += '</td></tr>'
        if currentPage is not None:
            ret += '<input type="hidden" name="XXredirect_internalXX" value="%s">' % currentPage
        return ret + '<tr><td><button name="save">Save</button></td></tr></form>'

    # TODO: When google tables are removed and queryResults are the main table,
    #        REMOVE the query_results parameters. Now it should always be True.
    def getMappedData(self, method_ref, response, args=[], query_results=False):
        if len(response) == 0:
            return
        method_props = dir(method_ref)
        map = None
        onlyshowmap = (False if 'onlyshowmap' not in method_props else method_ref.onlyshowmap)
        if 'map' in method_props:
            map = method_ref.map
        if map is None:
            map = {}

        if not isinstance(response[0], dict):
            tempResponse = []
            for row in response:
                tempResponse.append({'value': row})
            response = tempResponse

        rows = []
        tempheader = response[0].copy()

        for col in map:
            if 'show' in map[col] and map[col]['show'] is False and col in tempheader:
                del (tempheader[col])
        cols = [None] * (len(map) if onlyshowmap else len(tempheader))
        fullmap = {}
        appendvalues = []
        for col in map:
            if 'order' in map[
                    col]:  # and ((col in tempheader or onlyshowmap) or ('value' in map[col] and hasattr(map[col]['value'],'__call__'))):
                place = map[col]['order']
                cols[place] = self.getcol(col, map[col])
                fullmap[col] = place
                if col in tempheader:
                    del (tempheader[col])
            elif col not in tempheader and 'value' in map[col] and (
                    hasattr(map[col]['value'], '__call__') or hasattr(map[col]['value'], 'produce')):
                appendvalues.append(col)
                if not onlyshowmap:
                    cols.append(None)
            elif 'show' in map[col] and map[col]['show'] is False:
                pass
            elif col not in tempheader:
                self.logger.warning('column not found in data %s', col)
        currpos = 0
        if onlyshowmap is False:
            for key in tempheader:
                while cols[currpos] is not None:
                    currpos += 1
                cols[currpos] = self.getcol(key, map[key] if key in map else None)
                value = tempheader[key]
                if cols[currpos]['type'] == 'string' and not (key in map and 'value' in map[key]):
                    if isinstance(value, bool):
                        cols[currpos]['type'] = 'boolean'
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    elif isinstance(value, (int, long)) or (isinstance(value, str) and value.isdigit()):
                    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                    # elif isinstance(value, int) or (isinstance(value, str) and value.isdigit()):  # noqa: E115 - remove this noqa comment after migration cleanup
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        cols[currpos]['type'] = 'number'
                fullmap[key] = currpos
        for key in appendvalues:
            while len(cols) > currpos and cols[currpos] is not None:
                currpos += 1
            cols[currpos] = self.getcol(key, map[key])
            fullmap[key] = currpos
        ret = {}
        ret['cols'] = cols
        response = common.customtransform(response)
        for row in response:
            tempc = [None] * len(fullmap)
            for key in appendvalues:
                row[key] = key
            for key in fullmap:
                value = row[key] if key in row else ''
                if cols[fullmap[key]]['type'] == 'string' or cols[fullmap[key]]['type'] == 'html':
                    if key in map and 'value' in map[key] and hasattr(map[key]['value'], '__call__'):
                        value = map[key]['value'](row, key)
                    elif key in map and 'value' in map[key] and hasattr(map[key]['value'], 'produce'):
                        value = map[key]['value'].produce(row, '/' + ('/'.join(args)), method_ref.address)
                    else:

                        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                        if hasattr(value, 'encode') and sys.version_info[0] < 3:
                            value = value.encode('utf-8')
                        else:
                        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                            value = str(value)
                        if key in map and 'value' in map[key]:
                            value = common.evalString(map[key]['value'].replace('{{value}}', value), row)
                            if hasattr(method_ref, 'address'):
                                value = value.replace('{{module}}', '/'.join(method_ref.address.split('/')[:-1]))

                elif cols[fullmap[key]]['type'] == 'boolean':
                    value = bool(value)
                if key in fullmap:
                    if len(tempc) > fullmap[key]:
                        if query_results:
                            tempc[fullmap[key]] = value
                        else:
                            tempc[fullmap[key]] = {'v': value}

            if query_results:
                rows.append(tempc)
            else:
                rows.append({'c': tempc})

        ret['rows'] = rows
        return ret

    def renderTable(self, function, data=None, args=[], query_results=None, kwargs=None):
        if query_results:
            from . import restful
            # data is the data from the function
            title = query_results['title']
            table = query_results['table']
            data_source = query_results['data']
            table_filter = query_results['filter']
            arg_based = len(args) > 0
            columns = table['columns']
            field_count = len(columns)
            functions = []
            page_builder = restful.QueryResults()
            if data_source.get('url', None):
                data_source['db_address'] = '/'.join([function.address[:function.address.rfind('/')],
                                                      data_source['url']] + [a for a in args if a not in function.address])
            else:
                data_source['db_address'] = function.address

            if data_source.get('record_per_page', None) is None:
                data_source['record_per_page'] = 100

            record_per_page = data_source['record_per_page']

            result = {'fields': [info[0] for info in columns],
                      # TODO: how is this being use in js
                      'columns': columns,
                      'data': copy.deepcopy(data_source)}

            id_field = data_source.get('id_field', '_id')

            default_query = data_source.get('default_query', None)
            if default_query is None:
                if arg_based:
                    default_query = '{} = {}'.format(id_field, args[-1])
                elif kwargs.get('what', None):
                    default_query = kwargs['what']
            elif '{{value}}' in default_query:
                if arg_based:
                    default_query = data_source['default_query'].replace('{{value}}', str(args[-1]))
                else:
                    default_query = data_source['default_query'].replace('{{value}}', '')
            result['data']['default_query'] = default_query

            if data_source.get('url', None):
                # TODO: see if can get method name some how
                # TODO: Are the arguments reversed?
                result['pageScript'] = restful.get_page_script(query_results['file'], getattr(function, 'address'))

            default_sort = ''
            if table.get('sort', None):
                table_sort = table['sort']
                if 'default' in table_sort:
                    default_sort = '{} {}'.format(table_sort['default']['column'], table_sort['default']['direction'])
            else:
                table_sort = {'enabled': True}

            multi_select = table.get('multi_select', None)
            if multi_select:
                field_count += 1
                functions.append('multi_select')

            result_desc = None
            if default_query is not None:
                result_desc = restful.run_internal('core', 'post_queryResults_dataChange', *args,
                                                   **{'query': default_query,
                                                      'fields': result['fields'],
                                                      'return_count': record_per_page,
                                                      'columns': columns,
                                                      'multi_select': multi_select,
                                                      'page': 1,
                                                      'id_field': id_field,
                                                      'sort': default_sort,
                                                      'db_address': data_source['db_address'],
                                                      'data': data})

            saved_queries = None
            if table_filter:
                functions.append('filter')
                result['filter'] = {'search_keys': table_filter['options'],
                                    'always_use_default_query': 'always_use_default_query' in table_filter
                                                                and table_filter['always_use_default_query'],
                                    'allow_empty_query': 'allow_empty_query' in table_filter
                                                         and table_filter['allow_empty_query']}

                if table_filter.get('saved_queries', None) is not None:
                    from . import mongorestful
                    if not self.temp_mongo:
                        self.temp_mongo = mongorestful.MongoRestful(self.config.mongoserver,
                                                                    self.config.mongo_user,
                                                                    self.config.mongo_pass,
                                                                    self.config.meta_db)
                    saved_queries_page = table_filter['saved_queries']
                    if 'user' not in kwargs:
                        for key, value in self.userdata.items():
                            if isinstance(value, dict):
                                kwargs['user'] = value.get('name', None)
                                if kwargs['user'] is not None:
                                    break

                    saved_queries = sorted(self.temp_mongo.getUserQueries(args, kwargs, saved_queries_page),
                                           key=lambda k: k['name'])
                    functions.append('filter_savedqueries')
                    result['user'] = kwargs['user']
                    result['saved_queries'] = {'queries': saved_queries,
                                               'page': saved_queries_page}

            download = data_source.get('download', False)
            if download:
                functions.append('download')

            # no paging: no table_filter and total < record_per_page
            #            table_filter and allow_empty_query and default_query == '' and total < record_per_page
            paging = True
            if ((table_filter and table_filter.get('allow_empty_query', False)
                 and default_query == ''
                 and result_desc.get('total', record_per_page + 1) <= record_per_page)
                    or (not table_filter and result_desc.get('total', record_per_page + 1) <= record_per_page)):
                paging = False

            if paging:
                functions.append('pager')

            edit = table.get('edit', None)
            if edit:
                functions.append('edit')

            tmp_title = table.get('title', None)
            if tmp_title is None:
                table_title = title.replace('{{value}}', args[0]) if arg_based else title
            else:
                table_title = tmp_title

            html_filter = page_builder.create_filter(table_filter, saved_queries)
            html_select_actions = page_builder.create_select_actions(multi_select)
            html_actions = page_builder.create_actions(table.get('actions', None), args)

            result.update(page_builder.create_html(columns, field_count, table_title, id_field, None,
                                                   result_desc, html_filter, html_select_actions, download,
                                                   paging, multi_select, table_sort, edit, html_actions,
                                                   None if result_desc is None else result_desc.get('first_row')))

            result['multi_select'] = multi_select
            result['functions'] = functions
            result['record_per_page'] = record_per_page
            # TODO: Consider if I really need to pass tbody
            result['id_field'] = id_field
            result['sort'] = default_sort

            tmpl = self.env.get_template('query_results_frame.html')
            response = tmpl.render(data=result, args=args[2:], kwargs=kwargs)
            return response

        elif isinstance(data, (dict, common.CustomDict)):
            tmpl = self.env.get_template('single_table.html')

            formatted_data = {}
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(data):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in data.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                formatted_key = (key.replace('_', ' ')
                                 .replace('number', '#')
                                 .replace('perc_', '%_')
                                 .replace('percent', '%')
                                 .title())
                if formatted_key.startswith('No '):
                    formatted_key = formatted_key.replace('No ', '# ')

                formatted_data[formatted_key] = value

            response = tmpl.render(data=formatted_data)
            return response
        else:
            from lapinpy import restful

            func_props = dir(function)
            title = ''

            if 'title' in func_props:
                title = function.title

            if len(data) > 0:
                try:
                    temp = self.getMappedData(function, data, args, True)
                except Exception as e:  # noqa: F841
                    temp = {}

                if title == '':
                    title = function.__name__
                if temp:
                    realRows = temp['rows']
                    columns = []
                    for col in temp['cols']:
                        # TODO: Is this intentional? Would `string` be a key in the column?
                        type = 'string'
                        if type in col:
                            type = col['type']
                            if type == 'boolean':
                                type = 'bool'
                            elif type == 'int' or type == 'integer':
                                type = 'number'
                        columns.append([col['label'], {'type': type}])

                    table_data = realRows
                    html = (restful.QueryResults().create_html(title=title,
                                                               columns=columns,
                                                               field_count=len(columns),
                                                               generated_data=table_data)['html'])
                    temp['rows'] = html
                else:
                    html = '<h3>{}<h3>'.format(title)
            else:
                html = '<h3>{}<h3>'.format(title)

            return str(html)

    def googleChartify(self, response, method_ref, args, kwargs):
        data = self.getMappedData(method_ref, response, args)
        googlechartresponse = {'table': {}}
        if data is not None:
            googlechartresponse['table']['cols'] = data['cols']
            googlechartresponse['table']['rows'] = data['rows']
        googlechartresponse['reqId'] = kwargs['tqx'].replace('reqId:', '')
        return 'google.visualization.Query.setResponse(%s);' % json.dumps(googlechartresponse, default=jsonify)

    def run_method(self, module, method, *args, **kwargs):
        methodcall = getattr(self.apps[module], method)
        method_attr = dir(methodcall)
        if 'paged' in method_attr:
            return PageResponse(methodcall(args, kwargs), module)
        return methodcall(args, kwargs)

    def reloadApp(self, file):
        appName = os.path.splitext(os.path.split(file)[1])[0]
        old_mods = {}
        if appName in self.apps:
            oldApp = self.apps[appName]
            modules_to_delete = []
            for mod_name, mod in sys.modules.items():
                if hasattr(mod, '__appimport__') and mod.__file__.startswith(oldApp.location):
                    old_mods[mod_name] = sys.modules[mod_name]
                    print('deleting module {} at {}'.format(mod_name, id(sys.modules[mod_name])))
                    # del sys.modules[mod_name]
                    modules_to_delete.append(mod_name)
            for mod_name in modules_to_delete:
                del sys.modules[mod_name]
        application = self.loadApp(file)
        new_mods = {}
        for mod_name in old_mods:
            if mod_name in sys.modules:
                new_mods[mod_name] = sys.modules[mod_name]
            sys.modules[mod_name] = old_mods[mod_name]
        if application.appname in self.apps:
            oldApp = self.apps[application.appname]
            oldApp.stop()
            del oldApp
        for mod_name in new_mods:
            sys.modules[mod_name] = new_mods[mod_name]

        for mod_name in old_mods:
            if mod_name not in new_mods:
                del sys.modules[mod_name]
        del old_mods
        del new_mods
        self.apps[application.appname] = application
        self.reloadUrls()
        self.cachedMenus = {}
        if application.appname == 'core':
            application.restserver = self
            self.core = application
        for app in self.loadCallbacks:
            self.loadCallbacks[app]()
        if self.currentImporter is not None:
            ret = self.currentImporter.loaded_files
            self.currentImporter = None
            return ret

    def loadApp(self, file, obj=None):
        from lapinpy import restful
        full_path = os.path.realpath(file)
        if os.path.isfile(file):
            applicationName = os.path.split(file)[1].replace('.py', '')
            module = applicationName
        else:
            applicationName = file.split('.')[-1]
            module = file
        env = None
        if obj is None:
            # TODO: Why are we opening a file handle without using it or closing it? This is causing a resource leakage.
            # fin = open(file)
            try:
                if os.path.isfile(file):
                    # TODO: AJTRITT: self.rootdir was previously set to be the directory that contains
                    # the lapinpy package. I think this means there is a dependency on code outside the
                    # lapinpy package directory. It looks like there might be a dependency on apps that
                    # sit in the same directory as the lapinpy package directory.
                    if file != full_path or not full_path.startswith(self.rootdir):
                        '''this is a linked app, include all of its other python scripts'''
                        self.currentImporter = AppImportFinder(os.path.dirname(full_path), applicationName)
                        sys.meta_path = [self.currentImporter] + [path for path in sys.meta_path if not isinstance(path, AppImportFinder)]
                        # sys.meta_path = [self.currentImporter]
                        # sys.path.insert(1,os.path.dirname(full_path))

                    if sys.version_info[0] < 3:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                        import imp
                        app = imp.load_source(applicationName, file)
                    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
                    else:
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                        import importlib.util

                        spec = importlib.util.spec_from_file_location(applicationName, file)
                        app = importlib.util.module_from_spec(spec)
                        sys.modules[applicationName] = app
                        spec.loader.exec_module(app)
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup

                else:
                    if sys.version_info[0] < 3:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                        import imp
                        app = imp.load_source(file, file)
                    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
                    else:
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                        import importlib

                        #self.logger.critical("FILE: " + str(file))
                        app = importlib.import_module(file)
                    full_path = app.__file__

                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            except Exception as e:  # noqa: F841
                sys.meta_path = []
                # fin.close()
                raise
            # Clearing out `sys.meta_path` causes imports to fail in py3
            # sys.meta_path = []
            attributes = dir(app)
            className = None
            for attr_name in attributes:
                attr = getattr(app, attr_name)
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                try:
                    if isinstance(attr, (type, types.ClassType)):
                        if attr.__module__ == module and issubclass(attr, restful.Restful):
                            className = attr_name
                            break
                except AttributeError as e:  # noqa: F841
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    if isinstance(attr, type):
                        if attr.__module__ == module and issubclass(attr, restful.Restful):
                            className = attr_name
                            break
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if className is None:
                return

            application = getattr(app, className)
            if full_path.startswith(self.rootdir):
                # if loading an application that is actually a submodule of this package
                application = application(self.config)
            else:
                application = application(self.configManager.get_settings(applicationName))
                application.loaded_imports = self.currentImporter.loaded_files if self.currentImporter is not None else {}
        else:
            application = obj
        template_path = os.path.split(full_path)[0] + '/templates'
        if os.path.exists(template_path):
            env = Environment(loader=FileSystemLoader(template_path))
            env.filters['jsonify'] = to_json
        application.cronMethods = []
        if hasattr(application, 'menus'):
            newMenus = []
            for menu in application.menus:
                if 'href' in menu and 'title' in menu:
                    if 'permissions' not in menu:
                        menu['permissions'] = []
                    newMenus.append(menu)
            application.menus = newMenus
        else:
            application.menus = []
        application.ui_mappings = {}
        application.loaded_time = datetime.datetime.now()
        application.last_modified = datetime.datetime.fromtimestamp(
            os.path.getmtime(full_path)) if obj is None else datetime.datetime.now()
        application.location = os.path.dirname(full_path)
        application.appname = applicationName
        application.address = applicationName
        application.searchMethods = {}
        onLoad = []

        '''Generate all the app info from the methods, like cron events, menus'''
        methods = dir(application)
        permissions = {}
        for methodName in methods:
            method = getattr(application, methodName)
            if not hasattr(method, '__func__'):
                continue
            methodProps = dir(method)
            if 'usewhen' in methodProps:
                key, value = method.usewhen
                if key not in self.config or self.config[key] != value:
                    if key not in self.config or (isinstance(value, (list, dict)) and self.config[key] not in value):
                        continue
            if methodName.count('_') > 0:
                to_path = '/%s/%s' % (applicationName, methodName.split('_', 1)[1])
                method.__func__.address = to_path
            perms = []
            if 'search' in methodProps:
                application.searchMethods[method.search] = method.address
            if 'permissions' in methodProps:
                for perm in method.permissions:
                    permissions[perm] = False
                perms = method.permissions
            if 'is_ui' in methodProps and method.is_ui:
                url_path = method.url_path if 'url_path' in methodProps and method.url_path is not None else to_path
                application.ui_mappings[url_path] = {'rest_url': url_path, 'method': method}
                if 'menuname' in methodProps:
                    application.menus.append(
                        {'href': url_path, 'title': method.menuname, 'permissions': perms, 'order': method.order})
                method.__func__.address = url_path
                if 'template' in methodProps and env is not None and method.template != 'page_table.html':
                    method.__func__.env = env
            if 'cron' in methodProps:
                method.__func__.nextEvent = self.getNextEvent(method.cron)
                method.__func__.enabled = application.cron_enabled
                application.cronMethods.append(method)
            if 'call_on_finish' in methodProps:
                self.loadCallbacks[applicationName] = method
            if 'call_on_finish_single' in methodProps:
                onLoad.append(method)
            if 'is_async' in methodProps:
                queue = self.queueManager.get_queue(applicationName + '/' + methodName,
                                                    description=method.async_description, resources=['internal'])
                method.old_function.queue = queue

        if hasattr(self, 'core'):
            corepermissions = self.core.get_permissions(None, None)
            permhash = {}
            for row in corepermissions:
                permhash[row['name']] = True
            for perm in permissions:
                if perm not in permhash:
                    self.core.post_permission(None, {'name': perm})
            self.core.addModule(application)
        for meth in onLoad:
            meth()
        return application

    def reloadUrls(self):
        self.rest_ui_components = {}
        self.search_components = {}
        for app in self.apps:
            self.rest_ui_components.update(self.apps[app].ui_mappings)
            self.search_components.update(self.apps[app].searchMethods)

    def getAppFiles(self, files):
        for file in files:
            if os.path.isdir(file):
                for ifile in self.getAppFiles(os.listdir(file)):
                    yield file + '/' + ifile
            elif os.path.isfile(file):
                if file.endswith('py'):
                    yield file
            else: # assume file is a module name
                yield file

    def loadApps(self, files):
        logger = self.logger
        for file in self.getAppFiles(files):
            try:
                application = self.loadApp(file)
                self.apps[application.appname] = application
            except Exception:
                logger.critical('failed to load application %s' % file)

        self.core.put_permission_tree(None, None)
        if not hasattr(self.config, 'enable_cron') or self.config.enable_cron:
            cherrypy.process.plugins.BackgroundTask(60, self.cron_thread).start()
            logger.info('started cron events')
        cherrypy.process.plugins.BackgroundTask(20, self.task_thread).start()
        cherrypy.process.plugins.BackgroundTask(1, self.auto_reload_thread).start()
        self.reloadUrls()
        for app in self.loadCallbacks:
            self.loadCallbacks[app]()

    def task_thread(self):
        task = self.queueManager.next(['internal'])
        while task is not None:
            app, method_name = task['queue'].split('/')
            method = getattr(self.apps[app], method_name)
            try:
                method.old_function(self.apps[app], *task['data']['args'], **task['data']['kwargs'])
            except Exception as e:
                self.queueManager.fail(task['tid'], str(e))
            else:
                self.queueManager.finished(task['tid'])
            task = self.queueManager.next(['internal'])

    def auto_reload_thread(self):
        for app in self.apps:
            application = self.apps[app]
            file = os.path.join(application.location, '%s.py' % app)
            try:
                application.last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file))
            except OSError:
                continue
            if application.auto_reload and application.last_modified > application.loaded_time:
                try:
                    self.logger.info('reloading app %s', file)
                    self.reloadApp(file)
                except Exception:
                    application.loaded_time = datetime.datetime.now()
                    self.logger.critical('failed to compile module %s' % file)
        self.configManager.check_for_changes()

    def fillJob(self, ret, app, method_name):
        ret.rest_address = self.config.url + '/api/core/job'
        ret.job_path = os.path.join(ret.job_path, app, method_name, 'current')
        if ret.permissions is not None:
            ret.environment = ""
            if hasattr(self.config, 'jamo_token'):
                ret.environment += 'export JAMO_URL="{jamo_url}" JAMO_TOKEN="{jamo_token}"\n'.format(
                    jamo_token=self.config.jamo_token, jamo_url=self.config.jamo_url)
            if hasattr(self.config, 'pipeline_token'):
                ret.environment += 'export PIPELINE_URL="{pipeline_url}" PIPELINE_TOKEN="{pipeline_token}"\n'.format(
                    pipeline_token=self.config.pipeline_token, pipeline_url=self.config.pipeline_url)
        return ret

    def run_cron_method(self, method, force=False, app=None):
        now = datetime.datetime.now().replace(microsecond=0)
        if method.nextEvent < now or force:
            if method.enabled or force:
                try:
                    if hasattr(method, 'waitFor'):
                        if self.core.get_job([method.wait_for], None)['ended_date'] is None:
                            method.__func__.nextEvent = self.getNextEvent(method.cron)
                            return
                    method.__func__.lastRan = now
                    ret = method()
                    if isinstance(ret, Job):
                        if app is None:
                            app = 'misc'
                        ret = self.fillJob(ret, app, method.__name__)
                        method.__func__.wait_for = ret.run()
                        method.__func__.lastJobName = ret.job_name
                except Exception as e:  # noqa: F841
                    self.logger.critical('cron method failed to run')
            method.__func__.nextEvent = self.getNextEvent(method.cron)

    def cron_thread(self):
        for app in self.apps:
            application = self.apps[app]
            for method in application.cronMethods:
                self.run_cron_method(method, app=app)

    def addCronMethod(self, obj, method):
        event = self.getNextEvent(method.cron)
        self.cronEvents.append([event, obj, method])

    ''' min 0-59
        hour 0-23
        day 1-31
        month 1-12
        day of the week 0-6 monday is 1
    '''

    def getNextEvent(self, parms):
        min, hour, day, month, dow = parms
        now = datetime.datetime.now()
        allowed_days = self.__getAllowedValues(dow, 0, 6)
        while True:
            struc = [[now.minute, min, 0, 59], [now.hour, hour, 0, 23],
                     [now.day, day, 1, monthrange(now.year, now.month)[1]], [now.month, month, 1, 12],
                     [now.year, '*', now.year, now.year + 1]]
            onIdx = len(struc) - 1
            dir = -1
            while True:
                on = struc[onIdx]
                possibleValues = self.__getAllowedValues(on[1], on[2], on[3])
                if on[0] not in possibleValues or dir == 1 or onIdx == 0:
                    found = False
                    for i in possibleValues:
                        if i > on[0]:
                            found = True
                            on[0] = i
                            break
                    if not found:
                        on[0] = possibleValues[0]
                        dir = 1
                    else:
                        for i in range(0, onIdx):
                            on = struc[i]
                            possibleValues = self.__getAllowedValues(on[1], on[2], on[3])
                            on[0] = possibleValues[0]
                        break
                onIdx += dir
            ret = datetime.datetime(struc[4][0], struc[3][0], struc[2][0], struc[1][0], struc[0][0])
            if ret.isoweekday() % 7 in allowed_days:
                return ret
            '''This could be sped up if we change now to add a day, but for now it really isn't that big of a deal'''
            now = ret

    def __getAllowedValues(self, allowed, rangeS, rangeE):
        rangeE += 1
        ret = []
        parseValues = allowed.split(',')
        for value in parseValues:
            if value.count('-') == 1:
                tmp = value.split('-')
                ret.extend(list(range(int(tmp[0]), int(tmp[1]) + 1)))
            elif value.count('/') == 1:
                tmp = value.split('/')
                ret.extend(list(range(rangeS, rangeE, int(tmp[1]))))
            elif value == '*':
                return list(range(rangeS, rangeE))
            else:
                ret.append(int(value))

        return ret

    def _exit(self, signum, frame):
        cherrypy.engine.exit()
        for app in self.apps:
            self.apps[app].stop()

    def start(self, config, apps, block=True):
        # cherrypy.engine.timeout_monitor.unsubscribe()
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if sys.version_info[0] >= 3:
            # TODO: This is a bit hackish to get around setting the `Content-Type` header for JSON requests to
            #  `application/x-www-form-urlencoded` instead of `application/json`.
            #  We should remove this once we know all callers have migrated to using the appropriate headers...
            # BEGIN_HACK
            def handle_body():
                def body_processor(entity):
                    def process_urlencoded(entity):
                        """Copied from CherryPY source code to allow returning stream data."""
                        qs = entity.fp.read()
                        for charset in entity.attempt_charsets:
                            try:
                                params = {}
                                for aparam in qs.split(b'&'):
                                    for pair in aparam.split(b';'):
                                        if not pair:
                                            continue

                                        atoms = pair.split(b'=', 1)
                                        if len(atoms) == 1:
                                            atoms.append(b'')

                                        key = cherrypy._cpreqbody.unquote_plus(atoms[0]).decode(charset)
                                        value = cherrypy._cpreqbody.unquote_plus(atoms[1]).decode(charset)
                                        if key in params:
                                            if not isinstance(params[key], list):
                                                params[key] = [params[key]]
                                            params[key].append(value)
                                        else:
                                            params[key] = value
                            except UnicodeDecodeError:
                                pass
                            else:
                                entity.charset = charset
                                break
                        else:
                            raise cherrypy.HTTPError(
                                400, 'The request entity could not be decoded. The following '
                                     'charsets were attempted: %s' % repr(entity.attempt_charsets))

                        # Now that all values have been successfully parsed and decoded,
                        # apply them to the entity.params dict.
                        for key, value in params.items():
                            if key in entity.params:
                                if not isinstance(entity.params[key], list):
                                    entity.params[key] = [entity.params[key]]
                                entity.params[key].append(value)
                            else:
                                entity.params[key] = value
                        return qs

                    cherrypy.request.body.rawbody = process_urlencoded(entity)
                    self.logger.info(
                        'Request with `Content-Type: application/x-www-form-urlencoded`: path={}, caller={}'.format(
                            cherrypy.request.path_info, cherrypy.request.remote.ip))
                cherrypy.serving.request.body.processors['application/x-www-form-urlencoded'] = body_processor
            cherrypy.tools.handle_body = cherrypy.Tool('before_request_body', handle_body)

            cherrypy.config.update({'tools.encode.text_only': False,
                                    'tools.handle_body.on': True})
            # END_HACK
            # TODO: Uncomment below when removing `x-www-form-urlencoded` hack...
            # cherrypy.config.update({'tools.encode.text_only': False})

        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
        cherrypy.engine.autoreload.unsubscribe()
        if isinstance(config, dict):
            self.configManager = ConfigManager(None, config)
        else:
            self.configManager = ConfigManager(config)
        self.config = self.configManager.get_settings('lapinpy')
        if not (hasattr(self.config, 'verbose') and self.config.verbose):
            cherrypy.log.screen = None

        if not hasattr(self.config, 'url'):
            self.config.url = 'http://%s:%d' % (self.config.hostname, self.config.port)
            if 'shared' not in self.configManager.settings['lapinpy']:
                self.configManager.settings['lapinpy']['shared'] = {}
            self.configManager.settings['lapinpy']['shared']['url'] = self.config.url

        self.logger.info('starting lapinpy with the following settings:')
        # Dump the config to stdout
        for key in sorted(self.config.entries):
            if key == 'shared' or 'private' in key:
                pass
            elif '_token' in key or '_pass' in key:
                self.logger.info('%s : %s%s', key, str(self.config.entries[key])[:2], '*' * (len(str(self.config.entries[key])) - 2))
            else:
                self.logger.info('%s : %s', key, str(self.config.entries[key]))
        self.queueManager = QueueManager('core', self.config.queue_base if hasattr(self.config, 'queue_base') else '.')
        self.core = self.loadApp(resource_filename(__package__, 'core.py'))
        coreAppDir = os.path.join(resource_filename(__package__, 'apps'))
        for coreapp in os.listdir(coreAppDir):
            if coreapp.endswith('.py'):
                self.loadApp(os.path.join(self.rootdir, 'apps', coreapp))
        self.apps['core'] = self.core
        self.core.restserver = self
        self.loadApps(apps)
        for appItem in self.core.get_modules():
            file = appItem['path'] + '/' + appItem['name'] + '.py'
            if appItem['name'] in self.apps:
                continue
            try:
                application = self.loadApp(file)
                self.apps[application.appname] = application
            except Exception:
                self.logger.critical('failed to load application %s' % file)
        self.reloadUrls()
        self.cachedMenus = {}
        for app in self.loadCallbacks:
            self.loadCallbacks[app]()

        conf = {
            '/': {
                # TODO: AJTRITT: self.rootdir was previously set to be the directory that contains
                # the lapinpy package. I think this means there is a dependency on code outside the
                # lapinpy package directory. I don't know what those dependencies are, but we should
                # move them into the source package directory in the long run and avoid looking outside
                # of the package directory to ensure the package is distributable.
                #
                # I am wrapping setting tools.staticdir.root to be the directory above the package directory
                # to avoid breaking anything.
                'tools.staticdir.root': self.rootdir,
                'tools.proxy.on': True,
                'tools.proxy.local': 'X-Forwarded-Host',
                'tools.proxy.remote': 'X-Forwarded-For',
                # May need to override tools.proxy.local for laptop installs
                # that don't have an apache redirect going on?
                # 'tools.proxy.local': 'Host',
            },
            '/images': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': 'images',
            },
            '/scripts': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': 'scripts',
            }
        }
        corepermissions = self.core.get_permissions(None, None)
        permhash = {}
        for row in corepermissions:
            permhash[row['name']] = True
        if 'admin' not in permhash:
            self.core.post_permission(None, {'name': 'admin'})
        if hasattr(self.config, 'admins'):
            for admin in self.config.admins:
                cuser = self.core.get_user([admin], None)
                if cuser is None:
                    cuser = self.core._post_user(None, {'email': admin, 'name': admin})
                self.core.post_userpermission(None, {'user_id': cuser['user_id'], 'permission': 'admin'})

        if hasattr(self.config, 'oauthsecretfile'):
            self.flow = Flow.from_client_secrets_file(self.config.oauthsecretfile,
                                                      scopes=['https://www.googleapis.com/auth/userinfo.profile',
                                                              'https://www.googleapis.com/auth/userinfo.email',
                                                              'openid'],
                                                      redirect_uri='%s/oauth2callback' % (self.config.url))
        cherrypy.tree.mount(RestServer.Instance(), '', conf)
        cherrypy.server.unsubscribe()

        cherrypy.config.update({'log.screen': False,
                                'log.access_file': 'access.log',
                                'log.error_file': 'error.log'})

        logging.getLogger('cherrypy').propagate = False

        # The Metrics Service
        metrics_port = 9099 if not hasattr(self.config, 'metrics_port') else self.config.metrics_port
        start_http_server(metrics_port)
        self.logger.info('starting metrics server on port %d', metrics_port)

        if 'queue' in sys.modules:
            del sys.modules['queue']
        if hasattr(self.config, 'sslport'):
            self.logger.info('starting ssl server on port %d', self.config.sslport)
            server1 = cherrypy._cpserver.Server()
            server1.socket_port = int(self.config.sslport)
            server1.socket_host = getattr(self.config, 'host', '0.0.0.0')
            server1.ssl_module = 'pyopenssl'
            server1.ssl_certificate = self.config.ssl_cert
            server1.ssl_private_key = self.config.ssl_pkey
            server1.ssl_certificate_chain = self.config.ssl_chain
            if hasattr(self.config, 'thread_pool'):
                server1.thread_pool = self.config.thread_pool
            if hasattr(self.config, 'socket_queue_size'):
                server1.socket_queue_size = self.config.socket_queue_size
            server1.subscribe()
        if hasattr(self.config, 'port'):
            self.logger.info('starting server on port %d', self.config.port)
            server2 = cherrypy._cpserver.Server()
            server2.socket_port = int(self.config.port)
            server2.socket_host = getattr(self.config, 'host', '0.0.0.0')
            if hasattr(self.config, 'thread_pool'):
                server2.thread_pool = self.config.thread_pool
            if hasattr(self.config, 'socket_queue_size'):
                server2.socket_queue_size = self.config.socket_queue_size
            server2.subscribe()

        if hasattr(self.config, 'cypher_key'):
            from Crypto.Cipher import AES
            from Crypto.Hash import MD5
            m = MD5.new()
            global cipher
            m.update(self.config.cypher_key)
            cipher = AES.new(m.digest(), AES.MODE_ECB)
            del m
            del self.config.cypher_key

        signal(SIGINT, self._exit)
        cherrypy.engine.start()
        if block:
            self.logger.info('started engines, now I am blocking')
            cherrypy.engine.block()
