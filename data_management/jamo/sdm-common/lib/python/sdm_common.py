from builtins import str
from builtins import range
### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from builtins import object
import os
import imp


class Struct(object):
    def __init__(self, **entries):
        self.entries = entries
        self.__dict__.update(entries)

    def repr__(self):
        return '<%s>' % str('\n '.join('%s : %s' % (k, repr(v)) for (k, v) in self.__dict.items()))


def getToken(host):
    sdm_tokenfile = os.path.expanduser('~/.sdmtokens')
    if os.path.isfile(sdm_tokenfile):
        with open(sdm_tokenfile) as tHandle:
            onHost = False
            for line in tHandle:
                line = line.strip()
                if line == '':
                    next
                elif line.startswith('['):
                    hostL = line[1:-1].strip()
                    if hostL == host:
                        onHost = True
                elif onHost:
                    return line


class ValidationError(Exception):
    def __init__(self, error):
        Exception.__init__(self, error)
        self.error = error


class HttpException(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, str(code) + ':' + message)
        self.code = code
        self.message = message


def checkType(dataValue, expType):
    if isinstance(expType, (list, tuple)):
        good = False
        for exp in expType:
            if checkType(dataValue, exp):
                good = True
                break
        return good
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if expType in (str, str):
        expType = basestring
    elif expType in (int,):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if expType in (int,):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON2_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        if isinstance(dataValue, basestring) and dataValue.isdigit():
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    #     if isinstance(dataValue, str) and dataValue.isdigit():  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            dataValue = int(dataValue)
            return True
        elif isinstance(dataValue, int):
            dataValue = int(dataValue)
            return True
    return isinstance(dataValue, expType)


def checkdata(validator, data, expandKey='', allowExtra=True):
    errors = []
    if data is None:
        data = {}
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(validator, basestring) and validator.startswith('*'):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(validator, str) and validator.startswith('*'):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        expArgs = int(validator.split(':')[1]) if validator.count(':') > 0 else 0
        if expArgs > len(data):
            return ['You must pass in at least %d number of keys to this call' % expArgs]

    elif isinstance(data, dict) and isinstance(validator, dict):
        validate_next_level = False
        for key in validator:
            if key == '*':
                validate_next_level = True
                continue
            values = validator[key]
            if ('required' not in values or values['required'] is True) and key not in data:
                if 'default' in values:
                    data[key] = values['default']
                else:
                    keyPath = key if expandKey == '' else expandKey + '.' + key
                    errors.append('Missing required field: %s' % keyPath)
            if key in data:
                keyPath = key if expandKey == '' else expandKey + '.' + key
                data_value = data[key]
                if 'type' in values and values['type'] is list and not isinstance(data_value, list):
                    if data_value is None:
                        del data[key]
                    else:
                        errors.append('Attribute: "%s" has the wrong data type. Exected %s, got %s' % (
                            key, 'list', type(data_value).__name__))
                else:
                    values = values['validator'] if 'validator' in values else values['type']
                    errors.extend(checkdata(values, data_value, keyPath, allowExtra))
        if not allowExtra:
            for key in data:
                if validate_next_level:
                    errors.extend(checkdata(validator['*']['validator'], data[key], expandKey + '.' + key, allowExtra))
                elif key not in validator and key != '__auth':
                    errors.append('Invalid attribute:"%s"' % key)

    elif isinstance(data, list):
        if len(data) > 0:
            if 'type' in validator and validator['type'] == dict:
                validator = validator['validator']
            for i in range(len(data)):
                errors.extend(checkdata(validator, data[i], expandKey + '[%d]' % i, allowExtra))
    else:
        data_type = validator['type'] if isinstance(validator, dict) and 'type' in validator else validator
        if isinstance(data_type, dict):
            data_type = dict
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(data_type, tuple) and str in data_type:
            data_type = data_type + (basestring,)
        elif data_type is str:
            data_type = basestring
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        if not isinstance(data, data_type):
            name = data_type.__name__ if not isinstance(data_type, tuple) else ' or '.join(
                [val.__name__ for val in data_type])
            errors.append(
                'Attribute: "%s" has the wrong data type. Exected %s, got %s' % (expandKey, name, type(data).__name__))
    return errors


def getValidators(filepath):
    ret = {}
    mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
    if file_ext == '.py':
        if os.path.exists(filepath + 'c'):
            file_ext = '.pyc'
            filepath += 'c'
        else:
            src = imp.load_source(mod_name, filepath)
    if file_ext == '.pyc':
        src = imp.load_compiled(mod_name, filepath)
    if file_ext in ('.pyc', '.py'):
        for name in [x for x in dir(src) if not x.startswith('_')]:
            ret[name] = getattr(src, name)
    return ret
