### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future.utils import iteritems
from past.builtins import long
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from dateutil.parser import parse as parse_date
from bson.objectid import ObjectId
import re


class ValidationError(Exception):
    def __init__(self, error):
        Exception.__init__(self, error)
        self.error = error


class HttpException(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, str(code) + ':' + (','.join(message) if isinstance(message, list) else message))
        self.code = code
        self.message = message


def customtransform(ret, **methods):
    if isinstance(ret, dict):
        return CustomDict(ret, **methods)
    if isinstance(ret, list):
        return CustomList(ret, **methods)
    return ret


def prepend(*args, **kwargs):
    def outer(func):
        def inner(*iargs, **ikwargs):
            kwargs.update(ikwargs)
            return func(*(args + iargs), **kwargs)

        return inner

    return outer


# TODO: Is this being used?
def copy_args(function):
    def inner(self, *args, **kwargs):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        try:
            code = function.func_code
        except AttributeError as e:  # noqa: F841
            ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            code = function.__code__
            ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

        arg_names = code.co_varnames
        for i in range(1, code.co_argcount):
            name = arg_names[i]
            if name in kwargs:
                value = kwargs[name]
            elif i <= len(args):
                value = args[i - 1]
            else:
                value = function.__defaults__[i - (code.co_argcount - len(function.__defaults__))]
            setattr(self, name, value)
        return function(self, *args, **kwargs)

    return inner


class CustomDict(dict):
    def __init__(self, dic, **methods):
        self.dic = dic
        for method in methods:
            setattr(self, method, prepend(self)(methods[method]))

    def __getitem__(self, item):
        cDict = self.dic
        items = item.split('.')
        i = 0
        while i < len(items):
            key = items[i]
            if not isinstance(cDict, dict):
                return None
            if key in cDict:
                cDict = cDict[key]
            else:
                i2 = i + 1
                while i2 < len(items):
                    new_key = '.'.join(items[i:i2 + 1])
                    if new_key in cDict:
                        cDict = cDict[new_key]
                        i = i2
                        break
                    i2 += 1
                if i2 != i:
                    return None
            i += 1
        if cDict is not None and isinstance(cDict, (dict, list)):
            return customtransform(cDict)
        return cDict

    def __iter__(self):
        return self.dic.__iter__()

    def keys(self):
        return self.dic.keys()

    def __len__(self):
        return self.dic.__len__()

    def get(self, *items):
        for item in items:
            if self[item] is not None:
                return self[item]
        return None

    # TODO: Where are `methods` and `on` set?
    def __caller(self, *args, **kwargs):
        return self.methods[self.on](self.dic, *args, **kwargs)

    def __getattr__(self, name):
        if hasattr(self.dic, name):
            return getattr(self.dic, name)
        else:
            return customtransform(self.dic[name])

    def __repr__(self):
        return self.dic.__repr__()

    def __contains__(self, key):
        return self.__getitem__(key) is not None


class CustomList:
    def __init__(self, li, **methods):
        self.li = li
        self.methods = methods

    def __getattr__(self, name):
        return getattr(self.li, name)

    def __getitem__(self, item):
        return customtransform(self.li[item], **self.methods)

    def __str__(self):
        return str(self.li)

    def __iter__(self):
        class Iterat:

            def __init__(iself, lst):
                iself.lst = lst
                iself.i = -1

            def __iter__(iself):
                return iself

            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            def next(iself):
                return iself.__next__()
            ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

            def __next__(iself):
                if iself.i < len(iself.lst) - 1:
                    iself.i += 1
                    return customtransform(iself.lst[iself.i], **self.methods)
                raise StopIteration

        return Iterat(self.li)


class Struct:
    def __init__(self, **entries):
        self.entries = entries
        self.__dict__.update(entries)

    def __repr__(self):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        return '<%s>' % str('\n '.join('%s : %s' % (k, repr(v)) for (k, v) in iteritems(self.__dict__)))
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup
        # return '<%s>' % str('\n '.join('%s : %s' % (k, repr(v)) for (k, v) in self.__dict__.items()))
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup


def tokenize(string):
    inWhat = None
    inCount = 0
    strings = []
    currentString = ''
    i = 0
    while i < len(string):
        char = string[i]
        if char == '\\':
            currentString += string[i + 1]
            i += 2
            continue
        if inWhat is not None:
            if inWhat == '(':
                if char == '(':
                    inCount += 1
                elif char == ')':
                    inCount -= 1
            elif inWhat == char:
                inCount -= 1
            if inCount == 0:
                if char == ')':
                    currentString = tokenize(currentString)
                strings.append(currentString)
                currentString = ''
                inWhat = None
            else:
                currentString += char
            i += 1
            continue
        if char in ('"', "'", '('):
            if currentString != '':
                strings.append(currentString)
                currentString = ''
            inWhat = char
            inCount += 1
            i += 1
            continue
        if char in (' ', ',') and currentString != '':
            strings.append(currentString)
            currentString = ''
        elif char in ('>', '<', '=', '!'):
            if string[i + 1] == '=':
                char += string[i + 1]
                i += 1
            if currentString != '':
                strings.append(currentString)
                currentString = ''
            strings.append(char)

        elif not (char in (' ', ',') and currentString == ''):
            currentString += char
        i += 1
    if currentString != '':
        strings.append(currentString)
    ret = []
    for string in strings:
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(string, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(string, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if string.isdigit():
                string = int(string)
            elif string.lower() in ('true', 'false'):
                string = string.lower() == 'true'
        ret.append(string)
    return ret


date_fields = {'added_date', 'dt_to_purge', 'file_date', 'modified_date', 'metadata_modified_date'}

operators = {
    'gt': '$gt',
    'lt': '$lt',
    'gte': '$gte',
    'lte': '$lte',
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
    '=': '',
    'eq': '',
    'is': '',
    '!=': '$ne',
    'ne': '$ne',
    'in': '$in',
    'nin': '$nin',
    'like': '$regex',
    'exists': '$exists'
}


def convertToOID(obj):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(obj, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(obj, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        try:
            return ObjectId(obj)
        except Exception:
            return obj
    if isinstance(obj, list):
        return list(map(convertToOID, obj))
    if isinstance(obj, dict):
        return {k: convertToOID(v) for k, v in obj.items()}
    return obj


def stringify_tokens(tokens):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(tokens, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(tokens, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        return tokens
    elif isinstance(tokens, list):
        return " ".join([str(x) for x in tokens])
    else:
        return str(tokens)


def toMongoObj(tokens):
    query_string = stringify_tokens(tokens)
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(tokens, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(tokens, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        tokens = tokenize(tokens)
    ret = []
    i = 0
    keys = []
    len_tokens = len(tokens)
    while i < len_tokens:
        inOr = False
        key = tokens[i]
        if key in ('and', 'or'):
            if len(ret) == 0:
                raise Exception("Invalid placement of logical operator: %s, query: '%s'" % (key, query_string))
            if key == 'or':
                inOr = True
            i += 1
        if i >= len_tokens:
            raise Exception("Unexpected end of query, missing statement after and/or, query: '%s'" % query_string)
        key = tokens[i]
        if isinstance(key, tuple):
            funcname, args = key
            # TODO: Where is `functions` defined?
            if funcname not in functions:  # noqa: F821
                raise Exception("Invalid function used: %s, query: %s" % (funcname, query_string))
            else:
                key = functions[funcname](*args)  # noqa: F821
        if isinstance(key, list):
            t = toMongoObj(key)
            if inOr:
                if isinstance(ret[-1], list):
                    ret[-1].append(t)
                else:
                    ret[-1] = [ret[-1], t]
            else:
                ret.append(t)
            i += 1
            continue

        if i + 2 >= len_tokens:
            raise Exception("Unexpected end of query: '%s'" % query_string)
        op = tokens[i + 1]
        if op not in operators:
            raise Exception("Invalid comparison operator: '%s', query: '%s'" % (op, query_string))
        value = tokens[i + 2]
        # if this is one of our date fields, attempt to convert to a date
        if tokens[i] in date_fields and re.match(r"\d+-\d+-\d+", value):
            try:
                value = parse_date(value)
            except Exception:
                pass

        if op == '=' or op == 'eq':
            t = {key: value}
        elif op == 'is':
            if value == 'null':
                t = {"$or": [{key: {"$eq": None}}, {key: {"$eq": []}}]}
            else:
                if value == 'not' and tokens[i + 3] == 'null':
                    i += 1
                    t = {"$and": [{key: {"$ne": None}}, {key: {"$ne": []}}]}
                else:
                    raise Exception("Invalid usage of is: (only \"is null\" or \"is not null\" are allowed), query: '%s'" % query_string)
        elif op == 'like':
            # python2 version of what is in jamo_common [using number.Real]
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            if isinstance(value, (int, long, float)):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # if isinstance(value, (int, float)):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                value = str(value)
            t = {key: {operators[op]: value.replace('%', '.*'), '$options': 'i'}}
        elif op == 'exists':
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            t = {key: {operators[op]: value.lower() == 'true' if isinstance(value, basestring) else value}}
            ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup
            # t = {key: {operators[op]: value.lower() == 'true' if isinstance(value, str) else value}}
            ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
        else:
            t = {key: {operators[op]: value}}
        if inOr:
            temp = ret[-1]
            if isinstance(temp, list):
                temp.append(t)
            else:
                temp = [temp, t]
            ret[-1] = temp
        else:
            ret.append(t)
        i += 3
        keys.append(key)
    ret2 = {}
    # pre-add and to simplify the if else below
    ret2['$and'] = []
    for item in ret:
        if isinstance(item, list):
            ret2['$and'].append({'$or': item})
        else:
            for tItem in item:
                if tItem == '_id':
                    ret2[tItem] = convertToOID(item[tItem])
                elif tItem == '$and':
                    ret2[tItem].extend(item[tItem])
                elif tItem == '$or':
                    ret2['$and'].append({tItem: item[tItem]})
                else:
                    if keys.count(tItem) > 1:
                        ret2['$and'].append({tItem: item[tItem]})
                    else:
                        ret2[tItem] = item[tItem]
    # get rid of the and if we didn't use it
    if not ret2['$and']:
        del ret2['$and']
    # Attempt to pull out redundant $and and simplify enclosing arrays size 1.  Not a complete optimization
    ret1 = []
    while ret1 != ret2:
        ret1 = ret2
        if len(ret2) == 1 and '$and' in ret2 and len(ret2['$and']) == 1:
            # if we only have one object to the and, use the implied and in mongo
            ret2 = ret2['$and']
        if isinstance(ret2, list) and len(ret2) == 1:
            # if we've got down to a list of a single item, pull out the item
            ret2 = ret2[0]
    return ret2


def checkKey(value, condition):
    if not isinstance(condition, dict):
        if isinstance(value, list) and not isinstance(condition, list):
            return condition in value
        return value == condition
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    for key, kvalue in iteritems(condition):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # for key, kvalue in condition.items():  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        if key == '$regex':
            return None is not re.match(kvalue, value)
        if key == '$lt':
            return value < kvalue
        if key == '$lte':
            return value <= kvalue
        if key == '$gt':
            return value > kvalue
        if key == '$gte':
            return value >= kvalue
        if key == '$ne':
            return value != kvalue
        if key == '$in':
            return value in kvalue
        if key == '$nin':
            return value not in kvalue
        if key == '$exists':
            return kvalue == (value is not None)
    return False


def checkMongoQuery(data, query):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    for key, value in iteritems(query):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # for key, value in query.items():  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ret = True
        if key.startswith('$'):
            if key == '$and':
                ret = checkMongoQuery(data, value)
            elif key == '$or':
                ret = False
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                for skey, svalue in iteritems(value):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # for skey, svalue in value.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    if checkMongoQuery(data, {skey: svalue}):
                        ret = True
                        break
            elif key == '$not':
                ret = not checkMongoQuery(data, value)
            elif key == '$nor':
                ret = True
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                for skey, svalue in iteritems(value):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # for skey, svalue in value.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    if checkMongoQuery(data, {skey: svalue}):
                        ret = False
                        break
            else:
                ret = checkKey(getValue(data, key), value)
        else:
            ret = checkKey(getValue(data, key), value)
        if not ret:
            return False
    return True


def getValue(metadata, keys):
    on = metadata
    for key in keys.split('.'):
        if key not in on:
            return ''
        if isinstance(on[key], dict):
            on = on[key]
        else:
            return on[key]


def evalString(string, template):
    ret = ''
    inB = False
    key = ''
    for char in string:
        if char == '{' and not inB:
            inB = True
        elif char == '}' and inB:
            ret += str(getValue(template, key))
            key = ''
            inB = False
        elif inB:
            key += char
        else:
            ret += char
    return ret


def format_int(value):
    if value is None or value == 'None':
        return ''
    formatted_value = []
    reverse_value = list(str(value))[::-1]
    for index, digit in enumerate(reverse_value):
        if index != 0 and index % 3 == 0:
            formatted_value.append(',')
        formatted_value.append(digit)
    return ''.join(formatted_value[::-1])


def format_float(value, decimal_pnts=2):
    if value is None or value == 'None' or str(value).strip() == '':
        return ''

    value = str(value)

    if '.' not in value:
        value += '.{}'.format(''.zfill(decimal_pnts))

    parts = value.split('.')

    if len(parts[1]) < decimal_pnts:
        parts[1] = parts[1].ljust(decimal_pnts, '0')

    return '{}.{}'.format(format_int(parts[0]),
                          parts[1][:decimal_pnts])


def format_percent(value, is_fraction=False, decimal_pnts=2, include_symbol=True):
    if is_fraction:
        value = value * 100
    value = format_float(value, decimal_pnts)

    if include_symbol and value != '':
        return '{} %'.format(value)
    else:
        return value
