"""

    curl.py is a script containing Curl class that makes making curl calls easy

"""

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
import sys
if sys.version_info[0] >= 3:
    from types import SimpleNamespace
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # from typing import Any
### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from future import standard_library
standard_library.install_aliases()
# Also remove noqa from below module imports code
# above code installs a urllib reference for python2, if this is moved
# down to below the module imports, then the import urllib.* fail under py2
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

import json  # noqa:  E402
import urllib.request  # noqa:  E402
import urllib.error  # noqa:  E402
import urllib.parse  # noqa:  E402
import itertools  # noqa:  E402
from email.generator import _make_boundary as make_boundary  # noqa:  E402
import mimetypes  # noqa:  E402
from decimal import Decimal  # noqa:  E402
from time import sleep  # noqa:  E402


class CurlHttpException(Exception):

    def __init__(self, httpError):
        self.response = httpError.readlines()
        self.url = httpError.geturl()
        self.code = httpError.getcode()
        Exception.__init__(self, 'call to: %s threw code: %d %s' % (self.url, self.code, self.response))

    def __repr__(self):
        return 'call to: %s threw code: %d' % (self.url, self.code)


cachedCurls = {}


def handler(obj):
    if isinstance(obj, type):
        return obj.__name__
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return "%.2f" % obj
    # elif isinstance(obj, ...):
    #     return ...
    elif hasattr(obj, '__str__'):
        return str(obj)
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))


def __call(method, url, **kwargs):
    split = url.split('/', 3)
    server = '/'.join(split[:3])
    leftover = '/'.join(split[3:])
    if server in cachedCurls:
        curl = cachedCurls[server]
    else:
        curl = Curl(server, retry=0)
        cachedCurls[server] = curl
    if method == 'GET' and kwargs.get('cache', False):
        data = kwargs
        del kwargs['cache']
        return curl.get(leftover, data, cache=True)
    return curl._Curl__call(leftover, method, data=kwargs)


def get(url, *args, **kwargs):
    return __call('GET', url, *args, **kwargs)


def post(url, *args, **kwargs):
    return __call('POST', url, *args, **kwargs)


class Curl:

    def __init__(self, server, userName=None, userPass=None, oauth=None, token=None, appToken=None, retry=3, bearerToken=None, errorsToRetry=None, verify_cert=True):
        self.userData = None
        if userName is not None and userPass is not None:
            self.setupAuth(userName, userPass)
        elif oauth is not None:
            self.userData = "OAuth %s" % oauth
        elif bearerToken is not None:
            self.userData = 'Bearer {}'.format(bearerToken)
        elif token is not None:
            self.userData = "Token token=%s" % token
        elif appToken is not None:
            self.userData = 'Application %s' % appToken
        self.server = server
        self.cache = {}
        self.verify_cert = verify_cert
        self.retryAttempts = retry
        # TODO: Codes (500, 524) were hardcoded  where this gets used. Should the default be updated?
        self.errorsToRetry = [500]
        if errorsToRetry:
            if isinstance(errorsToRetry, (int, float)):
                self.errorsToRetry.append(errorsToRetry)
            else:
                self.errorsToRetry += errorsToRetry

    def setupAuth(self, userName, password):
        self.userData = "Basic " + (userName + ":" + password).encode("base64").rstrip()

    def __retry(self, request):
        for i in range(self.retryAttempts):
            try:
                return urllib.request.urlopen(request)
            except Exception:
                sleep(10)
        # we should store this in a file to be called if it is critical

    def __call(self, url, method, data=None, output='json', contenttype=None, verify=None, return_http_status_code=False):
        fullUrl = self.server + '/' + url
        if data is not None and method in ('GET', 'DELETE'):
            url_values = ''
            for key in data:
                url_values += '%s=%s&' % (key, data[key])
            if url_values != '':
                fullUrl += '?' + url_values
        elif data is None:
            data = ''

        req = urllib.request.Request(fullUrl)
        req.get_method = lambda: method

        if contenttype is not None:
            req.add_header('Content-type', contenttype)

        if isinstance(data, MultiPartForm):
            body = str(data)
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            if sys.version_info[0] < 3:
                req.add_data(body)
            else:
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                req.data = body.encode('utf-8')
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            # PY3 request will remove the `Content-length` header when setting data, so setting headers after setting
            # the data
            if contenttype is None:
                req.add_header('Content-type', data.get_content_type())
            req.add_header('Content-length', len(body))
        else:
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            if not isinstance(data, basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # if not isinstance(data, str):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                data = json.dumps(data, default=handler)
                if contenttype is None:
                    req.add_header('Content-type', 'application/json; charset=utf-8')
            else:
                data = data.replace("+", "%2B")
            if method not in ('GET', 'DELETE'):
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                if sys.version_info[0] < 3:
                    req.add_data(data)
                else:
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    req.data = data.encode('utf-8')
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                req.add_header('Content-length', len(data))

        if self.userData is not None:
            req.add_header('Authorization', self.userData)

        f = None

        try:
            #  Verify Cert?         Local verify
            #                   true    false   None
            #                  +-----------------------
            #  Global   True   |  V      !V      V
            #  verify   False  |  V      !V     !V
            #
            # So if the local is set to true, use that, otherwise use the global if the local is not set (i.e., None)
            if verify or (verify is None and self.verify_cert):
                f = urllib.request.urlopen(req)
            else:
                f = urllib.request.urlopen(req, context=ssl._create_unverified_context())
        except urllib.error.HTTPError as e:
            exception = CurlHttpException(e)
            if exception.code in self.errorsToRetry:
                f = self.__retry(req)
            if f is None:
                raise exception
        except (urllib.error.URLError, http.client.BadStatusLine) as e:
            # the server is not up maybe we should try again...
            f = self.__retry(req)
            if f is None:
                raise e
        response = f.read()
        f.close()
        if output == 'json':
            try:
                d = json.loads(response)
            except Exception as e:  # noqa: F841
                d = response
            return d
        if return_http_status_code:
            return response, f.getcode()
        return response

    def post(self, url, data=None, output='json', contenttype='application/json; charset=utf-8', verify=None,
             return_http_status_code=False, **kwargs):
        if kwargs is not None and len(kwargs) > 0:
            data = kwargs
        return self.__call(url, 'POST', data, output, contenttype, verify, return_http_status_code)

    def put(self, url, data=None, output='json', contenttype='application/json; charset=utf-8', verify=None, **kwargs):
        if kwargs is not None and len(kwargs) > 0:
            data = kwargs
        return self.__call(url, 'PUT', data, output, contenttype, verify)

    def delete(self, url, data=None, output='json', contenttype='application/json; charset=utf-8', verify=None, **kwargs):
        if kwargs is not None and len(kwargs) > 0:
            data = kwargs
        return self.__call(url, 'DELETE', data, output, contenttype, verify)

    def get(self, url, data=None, output='json', cache=False, verify=None, **kwargs):
        if kwargs is not None and len(kwargs) > 0:
            data = kwargs
        if cache:
            if url not in self.cache:
                self.cache[url] = self.__call(url, 'GET', data, output, verify=verify)
            return self.cache[url]
        return self.__call(url, 'GET', data, output, verify=verify)

    def toStruct(self, data):
        """Converts a `dict` to allow values to be accessed via dot-notation.
        e.g., `{'foo': 'bar'}`, where `foo` is accessed via `dict['foo']`, will be accessible via `struct.foo`.

        Unlike the PY2 implementation, the PY3 implementation will handle all nested `dict`s, rather than only one level
        deep and support non-`dict` values for the first level.

        :param data: Data to convert to struct (no-op for non dict)
        """

        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if sys.version_info[0] >= 3:
            if not isinstance(data, dict):
                return data
            return SimpleNamespace(**{key: self.toStruct(value) for key, value in data.items()})
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        else:
            temp = {}
            ret = lambda: 1  # noqa: E731
            for table in data:
                var = lambda: 2  # noqa: E731
                var.__dict__.update(data[table])
                temp[table] = var
            ret.__dict__.update(temp)
            return ret
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class MultiPartForm(object):
    """Accumulate the data to be used when posting a form."""

    def __init__(self):
        self.form_fields = []
        self.files = []
        self.boundary = make_boundary()
        return

    def get_content_type(self):
        return 'multipart/form-data; boundary=%s' % self.boundary

    def add_field(self, name, value):
        """Add a simple field to the form data."""
        self.form_fields.append((name, value))
        return

    def add_file(self, fieldname, filename, fileHandle, mimetype=None):
        """Add a file to be uploaded."""
        body = fileHandle.read()
        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        self.files.append((fieldname, filename, mimetype, body))
        return

    def __str__(self):
        """Return a string representing the form data, including attached files."""
        # Build a list of lists, each containing "lines" of the
        # request.  Each part is separated by a boundary string.
        # Once the list is built, return a string where each
        # line is separated by '\r\n'.
        parts = []
        part_boundary = '--' + self.boundary

        # Add the form fields
        parts.extend(
            [part_boundary,
             'Content-Disposition: form-data; name="%s"' % name,
             '',
             value,
             ]
            for name, value in self.form_fields
        )

        # Add the files to upload
        parts.extend(
            [part_boundary,
             'Content-Disposition: file; name="%s"; filename="%s"' %
             (field_name, filename),
             'Content-Type: %s' % content_type,
             '',
             body,
             ]
            for field_name, filename, content_type, body in self.files
        )

        # Flatten the list and add closing boundary marker,
        # then return CR+LF separated data
        flattened = list(itertools.chain(*parts))
        flattened.append('--' + self.boundary + '--')
        flattened.append('')
        return '\r\n'.join(flattened)
