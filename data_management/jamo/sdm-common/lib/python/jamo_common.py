### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import print_function
from builtins import input
from builtins import map
from builtins import str
from builtins import range
from past.builtins import basestring
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from builtins import object
from bson.objectid import ObjectId
import difflib
import json
import os
import subprocess
import sys
import tempfile
import yaml
from dateutil.parser import parse as parse_date
import numbers
import re


def expose(description, name=None):
    def inner(func):
        func.expose = True
        func.name = name
        func.description = description
        return func

    return inner


def parse_jamo_query(args):
    # deal with cases where the user has sent a query like key='string with spaces' (or key!='string with...)
    # so the list comprehension below can properly get the key.value as a separate argument to re-quote the value.
    args2 = []
    for item in args:
        if '=' in item and ' ' in item:
            arg = item.split('=', 1)
            args2.extend([arg[0] + '=', arg[1]])
        else:
            args2.append(item)

    # a mongo query was sent, don't mangle it
    if '{' in args[0]:
        return ' '.join(args)
    return ' '.join(["'" + x + "'" if ' ' in x and not x.startswith('(') else x for x in [re.sub('\'"', '', x) for x in args2]])


date_fields = {'added_date', 'dt_to_purge', 'file_date', 'modified_date', 'metadata_modified_date'}
mongo_ops = {'$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin', '$and', '$or'}


def convert_dates(query):
    # self.logger.info("converting %s, type=%s" % (str(query),str(type(query))))
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(query, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(query, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        # pagequery can return the whole query as a single string rather than an array of pieces
        try:
            # self.logger.info("attempting to parse %s, type=%s" % (str(query),str(type(query))))
            return parse_date(query)
        except Exception:
            return query
    elif isinstance(query, list):
        return list(map(convert_dates, query))
    ret = dict()
    for key in query.keys():
        if key in date_fields or key in mongo_ops:
            ret[key] = convert_dates(query[key])
        else:
            ret[key] = query[key]
    return ret


class ArgRunner(object):
    def __init__(self, name):
        self.name = name
        self.methods = [attr for attr in dir(self) if hasattr(getattr(self, attr), 'expose')]

    @expose('prints the help message')
    def help(self, args):
        sys.stderr.write('usage: %s <command> [<args>]\n' % self.name)
        sys.stderr.write('\nThe %s commands are:\n' % self.name)
        for method in self.methods:
            sys.stderr.write(' %-10s\t%s\n' % (method, getattr(self, method).description))
        sys.exit(2)

    def __call__(self, args):
        if len(args) == 0:
            self.help([])
        method = args[0]
        args = args[1:]
        if method not in self.methods:
            sys.stderr.write('''%s: '%s' is not a %s command.  Run '%s help' for more options\n''' % (
                self.name, method, self.name, self.name))
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes) > 0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n' % meth)
            sys.exit(64)
        if len(args) > 2 and args[-2] == '-f' and os.path.isfile(args[-1]):
            file_name = args[-1]
            args = args[:-2]
            with open(file_name) as fi:
                for line in fi.readlines():
                    args.append(line.rstrip())
        getattr(self, method)(args)


def editYaml(jsonO=None, fileLoc=None):
    f, fname = tempfile.mkstemp(suffix='.yml')
    if jsonO is not None:
        if isinstance(jsonO, str):
            string = jsonO
        else:
            string = yaml.safe_dump(jsonO, default_flow_style=False)
        f = open(fname, 'w')
        f.write(string)
        f.close()
    elif fileLoc is not None:
        fname = fileLoc
        with open(fname, 'r') as f:
            string = f.read()
    else:
        raise Exception('must pass in at least one argument')

    cmd = os.environ.get('EDITOR', 'vi') + ' ' + fname
    subprocess.call(cmd, shell=True)
    with open(fname, 'r') as f:
        ret = f.read()
    if ret == string:
        return None
    if fileLoc is None:
        os.unlink(fname)
    try:
        return yaml.full_load(ret)
    except Exception:
        value = input("Your yaml was invalid would you like to try again? [Y/n]")
        if value.lower() == 'n':
            sys.exit(1)
        else:
            return editYaml(ret)


def replaceKeys(data, *strings):
    ret = []
    for string in strings:
        ret.append(evalString(string, data))
    if len(ret) == 1:
        return ret[0]
    return ret


def replaceAllValues(data, obj):
    if isinstance(obj, list):
        ret = []
        for val in obj:
            ret.append(replaceAllValues(data, val))
        return ret
    elif isinstance(obj, dict):
        ret = {}
        for key, value in obj.items():
            ret[key] = replaceAllValues(data, value)
        return ret
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    elif isinstance(obj, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # elif isinstance(obj, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        return replaceKeys(data, obj)
    return obj


def evalString(string, template):
    ret = ''
    inB = False
    key = ''
    for char in string:
        if char == '{' and not inB:
            inB = True
        elif char == '}' and inB:
            t = getValue(template, key.strip())
            ret += str(t) if not isinstance(t, str) else t
            key = ''
            inB = False
        elif inB:
            key += char
        else:
            ret += char
    return ret


def getValue(metadata, keys):
    on = metadata
    for key in keys.split('.'):
        if key not in on:
            return ''
        if isinstance(on[key], dict):
            on = on[key]
        else:
            return on[key]
    return on


def editJson(jsonO=None, fileLoc=None):
    f, fname = tempfile.mkstemp(suffix='.json')
    if jsonO is not None:
        if isinstance(jsonO, str):
            string = jsonO
        else:
            string = json.dumps(jsonO, indent=4, separators=(',', ':'))
        f = open(fname, 'w')
        f.write(string)
        f.close()
    elif fileLoc is not None:
        fname = fileLoc
    else:
        raise Exception('must pass in at least one argument')

    cmd = os.environ.get('EDITOR', 'vi') + ' ' + fname
    subprocess.call(cmd, shell=True)
    with open(fname, 'r') as f:
        ret = f.read()
    if fileLoc is None and ret == string:
        return None
    if fileLoc is None:
        os.unlink(fname)
    try:
        return json.loads(ret)
    except Exception:
        value = input("Your json was invalid would you like to try again? [Y/n]")
        if value.lower() == 'n':
            sys.exit(1)
        else:
            return editJson(ret)


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


class CustomDict(object):
    def __init__(self, dic, **methods):
        self.dic = dic
        for method in methods:
            setattr(self, method, prepend(self)(methods[method]))

    def __delitem__(self, key):
        cDict = self.dic
        items = key.split('.')
        for i in items[:-1]:
            if not isinstance(cDict, dict) or i not in cDict:
                return
            cDict = cDict[i]
        del cDict[items[-1]]

    def __getitem__(self, item):
        leftovers = []
        if item.count('.') > 0:
            item, leftovers = item.split('.', 1)
        if item not in self.dic:
            return None
        ret = self.dic[item]
        if len(leftovers) > 0:
            return customtransform(ret)[leftovers]
        return ret

    def __setitem__(self, key, value):
        items = key.split('.')
        cDic = self.dic
        for key in items[:-1]:
            if key not in cDic:
                cDic[key] = {}
            cDic = cDic[key]
        cDic[items[-1]] = value

    def get(self, *items):
        for item in items:
            if self[item] is not None:
                return self[item]
        return None

    # TODO: This method won't as `methods` dict doesn't exist and neither `on`
    def __caller(self, *args, **kwargs):
        return self.methods[self.on](self.dic, *args, **kwargs)

    def __getattr__(self, name):
        if hasattr(self.dic, name):
            return getattr(self.dic, name)
        else:
            return customtransform(self.dic[name])

    def __contains__(self, key):
        return self[key] is not None


class CustomList(object):
    def __init__(self, li, **methods):
        self.li = li
        self.methods = methods

    def __getattr__(self, name):
        return getattr(self.li, name)

    def __getitem__(self, item):
        leftovers = []
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(item, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(item, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if item.count('.') > 0:
                item, leftovers = item.split('.', 1)
                item = int(item)
        # this will only return the first item, should return an array of all the values
        if isinstance(self.li, list):
            ret = self.li[0][item]
        else:
            ret = self.li[item]
        if len(leftovers) > 0:
            # TODO: This method doesn't make a lot of sense to me...Do we need to cast `leftovers` as `int`? Are we
            #  mixing data structures???
            return customtransform(self.li[item], **self.methods)[leftovers]
        return ret

    def __iter__(self):
        class Iterat(object):
            def __init__(iself, lst):
                iself.lst = lst
                iself.i = -1

            def __iter__(iself):
                return iself

            def __next__(iself):
                if iself.i < len(iself.lst) - 1:
                    iself.i += 1
                    return customtransform(iself.lst[iself.i], **self.methods)
                raise StopIteration

        return Iterat(self.li)


__pairs = {'(': ')', '{': '}', '[': ']'}
'''
    keywords:
        like -> {$regex: phrase}
        in -> {$in:[items]
        or -> $or:[conditions]
        and -> normal
        > -> gt
        < -> lt
        >= gte
        <= lte
   user = sdm and fastq_type=pooled or user = rqc
'''


def tokenize(string):
    inWhat = None
    inCount = 0
    strings = []
    currentString = ''
    i = 0
    funcname = None
    while i < len(string):
        char = string[i]
        if char == '\\':
            currentString += string[i + 1]
            i += 2
            continue
        if inWhat is not None:
            if inWhat in __pairs:
                if char == inWhat:
                    inCount += 1
                elif char == __pairs[inWhat]:
                    inCount -= 1
            elif inWhat == char:
                inCount -= 1
            if inCount == 0:
                if inWhat == '(':
                    currentString = tokenize(currentString)
                    if funcname is not None:
                        strings.append((funcname, currentString))
                        funcname = None
                        currentString = ''
                elif inWhat in ('{', '['):
                    currentString = json.loads(inWhat + currentString + __pairs[inWhat])
                if currentString != '':
                    strings.append(currentString)
                    currentString = ''
                inWhat = None
            else:
                currentString += char
            i += 1
            continue
        if char in ('"', "'", '(', '[', '{'):
            if currentString != '':
                if char == '(':
                    funcname = currentString
                else:
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
            if i + 1 >= len(string):
                raise Exception("Unexpected end of query: %s" % string)
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


def openFile(filename):
    print(filename, '--------')


functions = {
    'file': openFile
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


def toMongoSet(tokens):
    ret = {}
    query_string = stringify_tokens(tokens)
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(tokens, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(tokens, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        tokens = tokenize(tokens)
    len_tokens = len(tokens)
    for i in range(0, len_tokens, 3):
        if i + 2 >= len_tokens:
            raise Exception("Unexpected end of query: %s" % query_string)
        if tokens[i + 1] != '=':
            raise Exception("Invalid operator at pos %d, expecting =, query: %s" % (i + 1, query_string))
        ret[tokens[i]] = tokens[i + 2]
    return {'$set': ret}


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
                raise Exception("Invalid placement of logical operator: %s, query: %s" % (key, query_string))
            if key == 'or':
                inOr = True
            i += 1
        if i >= len_tokens:
            raise Exception("Unexpected end of query, missing statement after and/or, query: %s" % query_string)
        key = tokens[i]
        if isinstance(key, tuple):
            funcname, args = key
            if funcname not in functions:
                raise Exception("Invalid function used: %s, query: %s" % (funcname, query_string))
            else:
                key = functions[funcname](*args)
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
            raise Exception("Unexpected end of query: %s" % query_string)
        op = tokens[i + 1]
        if op not in operators:
            raise Exception("Invalid comparison operator: '%s', query: %s" % (op, query_string))
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
                    raise Exception('Invalid usage of is: (only "is null" or "is not null" are allowed)')
        elif op == 'like':
            # t = {key: {operators[op]: '^' + value.replace('%', '.*'), '$options': 'i'}}
            if isinstance(value, numbers.Real):
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
    # Attempt to pull out redundant $and and other bits.  Not a complete optimization
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


def toInt(args):
    if isinstance(args, list):
        return list(map(int, args))
    return int(args)


def toObjectId(args):
    if isinstance(args, list):
        return list(map(ObjectId, args))
    return ObjectId(args)


def openFile(args):
    with open(args[0]) as f:
        return [line.strip() for line in f.readlines()]


def stdin(args):
    return [line.strip() for line in sys.stdin.readlines()]


useableMethods = {
    'int': toInt,
    'oid': toObjectId,
    'file': openFile,
    'stdin': stdin,
}


def detokenize(token):
    if isinstance(token, tuple):
        funcname, args = token
        if funcname not in useableMethods:
            raise Exception('Method: %s is not a valid function' % funcname)
        return useableMethods[funcname](detokenize(args))
    elif isinstance(token, list):
        ret = []
        for tok in token:
            val = detokenize(tok)
            if isinstance(val, list):
                ret.extend(val)
            else:
                ret.append(val)
        return ret
    else:
        return token


def getQueries(args):
    jsonD = ' '.join(args)
    if jsonD.startswith('{'):
        jsonD = json.loads(jsonD)
        return [jsonD]
    else:
        tokens = tokenize(jsonD)
        new_tokens = []
        for token in tokens:
            if isinstance(token, tuple):
                funcname, args = token
                if funcname not in useableMethods:
                    raise Exception('Method: %s is not a valid function' % funcname)
                token = useableMethods[funcname](detokenize(args))
            new_tokens.append(token)
        for i in range(len(new_tokens)):
            if isinstance(new_tokens[i], list) and len(new_tokens[i]) > 60:
                ret = []
                items = new_tokens[i]
                for start in range(0, len(items), 50):
                    ret.append(toMongoObj(new_tokens[:i] + [items[start:start + 50]] + new_tokens[i + 1:]))
                return ret
        return [toMongoObj(new_tokens)]


def getQuery(args):
    if isinstance(args, list):
        jsonD = ' '.join(args)
    else:
        jsonD = args
    if jsonD.startswith('{'):
        jsonD = json.loads(jsonD)
        return [jsonD]
    else:
        tokens = tokenize(jsonD)
        new_tokens = []
        for token in tokens:
            if isinstance(token, tuple):
                funcname, args = token
                if funcname not in useableMethods:
                    raise Exception('Method: %s is not a valid function' % funcname)
                token = useableMethods[funcname](detokenize(args))
            new_tokens.append(token)
        return toMongoObj(new_tokens)


def checkKey(value, condition):
    if not isinstance(condition, dict):
        if isinstance(value, list) and not isinstance(condition, list):
            return condition in value
        return value == condition
    for key, kvalue in condition.items():
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
    for key, value in query.items():
        ret = True
        if key.startswith('$'):
            if key == '$and':
                ret = checkMongoQuery(data, value)
            elif key == '$or':
                ret = False
                for skey, svalue in value.items():
                    if checkMongoQuery(data, {skey: svalue}):
                        ret = True
                        break
            elif key == '$not':
                ret = not checkMongoQuery(data, value)
            elif key == '$nor':
                ret = True
                for skey, svalue in value.items():
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


class PageList(object):
    '''
    Pass the result of a pagequery into this to make an iterable over a pagequery result

    e.g:
        files =  PageList(curl.post('api/metadata/pagequery',query=my_query, fields=my_fields), curl)
        for f in files:
            # do something
    '''

    def __init__(self, page, curl, service='metadata', **methods):
        '''
        page: the pagequery result to iterate over
        curl: the Curl object from which the pagequery was called
        service: the application the pagequery was called on. This must also have a get_nextpage method
        methods: I am not sure what this is (ajtritt)
        '''

        self.page = page
        self.service = service
        self.current_list = page['records']
        self.cursor_id = page['cursor_id']
        self.total_record_count = page['record_count']
        self.curl = curl
        self.methods = methods

    def __getitem__(self, item):
        return customtransform(self.current_list[item], **self.methods)

    def __iter__(self):
        class Iterat(object):
            def __init__(iself):
                iself.i = -1
                iself.si = -1

            def __iter__(iself):
                return iself

            def __next__(iself):
                if iself.i < self.total_record_count - 1:
                    iself.i += 1
                    if iself.si + 2 > len(self.current_list):
                        iself.si = -1
                        self.current_list = self.curl.get('api/%s/nextpage/%s' % (self.service, self.cursor_id))[
                            'records']
                    iself.si += 1
                    return customtransform(self.current_list[iself.si], **self.methods)
                raise StopIteration

        return Iterat()

    def __len__(self):
        return self.total_record_count


class JiraUsers(object):
    # region Constant variables
    user_map = {2465: 'nrshapiro',
                3187: 'nzvenigorodsky',
                2309: 'tglavina',
                3313: 'sdeshpande',
                13: 'kmtaylor',
                2709: 'mlharmon',
                6705: 'lagoodwin',
                10037: 'mlharmon',
                116: 'kwbarry',
                21: 'cppennacchio',
                23: 'kwbarry',
                218: 'lagoodwin',
                2587: 'mlharmon',
                2586: 'mlharmon',
                53: 'lagoodwin'}
    # endregion Constant variables

    # region Methods
    # region Methods - Private
    def __init__(self):
        pass

    # endregion Methods - Private
    def get_pm_username_from_file_metadata(self, metadata):
        pm_username = None
        if ('proposal' in metadata
                and 'default_project_manager' in metadata['proposal']
                and 'email_address' in metadata['proposal']['default_project_manager']):
            pm_username = metadata['proposal']['default_project_manager']['email_address'].split('@')[0]
        elif ('project_collaborators' in metadata
              and 'project_manager_cid' in metadata['project_collaborators']
              and metadata['project_collaborators']['project_manager_cid'] in self.user_map):
            pm_username = self.user_map[metadata['project_collaborators']['project_manager_cid']]
        elif ('sequencing_project' in metadata
              and 'project_manager_cid' in metadata['sequencing_project']
              and metadata['sequencing_project']['project_manager_cid'] in self.user_map):
            pm_username = self.user_map[metadata['sequencing_project']['project_manager_cid']]
        elif ('sow_segment' in metadata
              and 'sequencing_project_manager_id' in metadata['sow_segment']
              and metadata['sow_segment']['sequencing_project_manager_id'] in self.user_map):
            pm_username = self.user_map[metadata['sow_segment']['sequencing_project_manager_id']]

        if pm_username is not None:
            return pm_username.strip()
        else:
            return pm_username

    def set_users_templates(self, data, metadata):
        # sets the assignee name in the jira data
        data = self.set_assignee_in_jira_data(data)

        # sets the watchers and removes them from data
        return self.set_watchers_templates(data, metadata)

    def set_watchers_pacbio(self, watchers, pm_username):
        return self.add_pm_to_watchers(watchers, pm_username)

    def set_watchers_templates(self, data, metadata):
        watchers = []
        if 'watchers' in data:
            template_watchers = data['watchers']
            for watcher in template_watchers:
                if watcher.isdigit() and int(watcher) in self.user_map:
                    watchers.append(self.user_map[int(watcher)])
                else:
                    watchers.append(watcher)
            del data['watchers']

        # data with watchers removed | watchers with pm_username added
        return data, self.add_pm_to_watchers(watchers, get_pm_from_metadata=metadata)

    def set_assignee_in_jira_data(self, data):
        if ('fields' in data
                and 'assignee' in data['fields']
                and 'name' in data['fields']['assignee']
                and data['fields']['assignee']['name'].isdigit()):
            data['fields']['assignee']['name'] = self.user_map[int(data['fields']['assignee']['name'])]

        return data

    def add_pm_to_watchers(self, watchers, pm_username=None, get_pm_from_metadata=None):
        if get_pm_from_metadata is not None:
            pm_username = self.get_pm_username_from_file_metadata(get_pm_from_metadata)

        if pm_username is not None and pm_username.strip() != '' and pm_username not in watchers:
            watchers.append(pm_username)

        return watchers
    # region Methods - Public
    # endregion Methods - Public
    # endregion Methods
