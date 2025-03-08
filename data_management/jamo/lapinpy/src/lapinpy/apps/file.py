### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from future.utils import iteritems
from builtins import str as text
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import cherrypy
import os
from lapinpy import restful


def str(value):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(value, text):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(value, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        return value.encode('utf-8')
    else:
        return value.__str__()


class File(restful.Restful):

    def getlink(row, args):
        if row['is_dir']:
            return '<a href="/file/files%s/%s">%s</a>' % (row['file_path'], row['file_name'], row['file_name'])
        return row['file_name']

    @restful.template('filebrowser.html')
    def get_files(self, args, kwargs):
        path = '/' + '/'.join(args)
        if not os.path.exists(path) or not os.path.isdir(path):
            return self.error(400, 'Path you provided is not a folder')
        files = os.listdir(path)
        ret = []
        folders = []
        folders.append(
            {'file_name': '..', 'file_path': path, 'is_dir': os.path.isdir(path), 'file_size': os.path.getsize(path)})
        for file in files:
            full_path = os.path.join(path, file)
            obj = {'file_name': file, 'file_path': path, 'is_dir': os.path.isdir(full_path),
                   'file_size': os.path.getsize(full_path)}
            if obj['is_dir']:
                folders.append(obj)
            else:
                ret.append(obj)
        ret = folders + ret
        if 'tq' in kwargs:
            ofset = int(kwargs['tq'].split('offset ')[1])
            return ret[ofset:]
        return ret

    @restful.raw
    def get_download(self, args, kwargs):
        typ = args[0]
        module = args[1]
        call = args[2]
        args = args[3:]
        data = restful.run_internal(module, 'get_%s' % call, *args, **kwargs)
        headers = {}
        onCol = 0
        if 'cursor_id' in data and 'records' in data:
            cursor = data['cursor_id']
            rows = data['record_count']
            data = data['records']
            while len(data) < rows:
                try:
                    data.extend(restful.run_internal(module, 'get_nextpage', cursor)['records'])
                except Exception:
                    break
        for i in range(len(data)):
            row = data[i]
            line = [''] * onCol
            for key, value in self.flatitterate(row):
                if key not in headers:
                    headers[key] = onCol
                    onCol += 1
                    line.append(value)
                else:
                    line[headers[key]] = value
            data[i] = line

        headerline = [''] * onCol
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for head, place in iteritems(headers):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for head, place in headers.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            headerline[place] = head
        cherrypy.response.headers['Content-Type'] = 'application/%s' % typ
        cherrypy.response.headers["Content-Disposition"] = 'attachment; filename=%s' % call + '.' + typ
        delimiter = ','
        if typ == 'tab':
            delimiter = '\t'
        ret = str(delimiter.join(headerline))
        for i in range(len(data)):
            ret += str('\n')
            for pos in range(len(data[i])):
                ret += str('"{}"'.format(str(data[i][pos])))
                if pos < len(data[i]):
                    ret += str(delimiter)
            data[i] = None
        return ret

    def flatitterate(self, dictonary):
        ### PYTHON2_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        for key, value in iteritems(dictonary):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, value in dictonary.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if isinstance(value, dict):
                for i_key, i_value in self.flatitterate(value):
                    yield (key + '.' + i_key, i_value)
            else:
                if isinstance(value, list):
                    value = str('"{}"'.format(str(',').join(map(str, value))))
                yield key, str(value)
