#!/usr/bin/env python2
from __future__ import print_function
import sys
import yaml, json
import os
import sdm_curl
from jamo_common import expose,customtransform,toMongoObj,tokenize, PageList

def parse_jamo_query(args):
    return ' '.join(["'"+x+"'" if ' ' in x and not x.startswith('(') else x for x in [x.strip('\'" ') for x in args]])


class QT():

    def __init__(self):
        self.curl = sdm_curl.Curl(os.environ.get('JAMO_HOST','https://jamo.jgi.doe.gov'))
        self.methods=[]
        self.methodMap={}
        for attr in dir(self):
            method = getattr(self,attr)
            if hasattr(method,'expose'):
                if method.name is not None:
                    self.methodMap[method.name]=attr
                    self.methods.append(method.name)
                else:
                    self.methods.append(attr)
        self.sources = {
                'jamo': {'url':'/api/metadata/pagequery', 'description':'Accesses the data available in JAMO.'},
                'ncbi': {'url':'/api/ncbi/pagequery', 'description':'Acesses the ncbi submission pipeline data.'},
                'jat': {'url':'/api/analysis/pagequery', 'description': 'Accesses the data avaialble in JAT.'}
        }

    def error(msg):
        sys.stderr.write(msg)
        sys.exit(2)

    @expose('Runs a query using the specified data source', 'from')
    def frome(self, args):
        if len(args)==0 or args[0] == 'help':
            sys.stderr.write( 'Usage: qt from <data source> select <fields> where <clause> [ as <json | yaml> ]\n\n')
            sys.stderr.write( 'The possible data sources are:\n')
            for source in self.sources:
                sys.stderr.write( ' %s\t%s\n'%(source, self.sources[source]['description']))
            sys.stderr.write( '\n')
            sys.exit(2)
        if args[0] not in self.sources:
            self.error('error: The provided source "%s" was not found. Please run qt from help\n')

        source = self.sources[args[0]]
        args = args[1:]
        if args[0]=='select':
            where_loc = args.index('where') if 'where' in args else None
            if where_loc is None or where_loc>=len(args)-1:
                sys.stderr.write('Sorry you must provide a query after the \'where\' keyword\n')
                sys.exit(1)
            outt = 'txt'
            if args[-2] =='as':
                outt = args[-1]
                args = args[:-2]
            fields = (''.join(args[1:where_loc])).split(',')
            queries = self.getQueries(args[where_loc+1:])
            for query in queries:
                files =  PageList(self.curl.post(source['url'] ,query=query, fields=fields), self.curl, service=source['url'].split('/')[2])
                if outt in ('json','yaml'):
                    outlis = []
                    for file in files:
                        tmp = {}
                        for field in fields:
                            tmp[field]=file[field]
                        outlis.append(tmp)
                    if outt == 'yaml':
                        print(yaml.safe_dump(outlis,default_flow_style=False))
                    elif outt == 'json':
                        print(json.dumps(outlis))
                else:
                    for file in files:
                        for field in fields:
                            sys.stdout.write('%s\t'%file[field])
                        sys.stdout.write('\n')
            return

    def toInt(args):
        if isinstance(args,list):
            return map(int,args)
        return int(args)

    def toObjectId(args):
        if isinstance(args, list):
            return map(ObjectId, args)
        return ObjectId(args)

    def openFile(args):
        with open(args[0]) as f: return [line.strip() for line in f.readlines()]

    def stdin(args):
        return [line.strip() for line in sys.stdin.readlines()]

    useableMethods = {
            'int':toInt,
            'oid':toObjectId,
            'file': openFile,
            'stdin': stdin,
        }

    def detokenize(self, token):
        ret = None
        if isinstance(token, tuple):
            funcname, args = token
            if funcname not in self.useableMethods:
                raise Exception('Method: %s is not a valid function'%funcname)
            return self.useableMethods[funcname](self.detokenize(args))
        elif isinstance(token, list):
            ret = []
            for tok in token:
                val = self.detokenize(tok)
                if isinstance(val, list):
                    ret.extend(val)
                else:
                    ret.append(val)
            return ret
        else:
            return token

    def getQueries(self, args):
        jsonD = parse_jamo_query(args)
        if jsonD.startswith('{'):
            jsonD = json.loads(jsonD)
            return [jsonD]
        else:
            tokens = tokenize(jsonD)
            new_tokens = []
            for token in tokens:
                if isinstance(token, tuple):
                    funcname, args = token
                    if funcname not in self.useableMethods:
                        raise Exception('Method: %s is not a valid function'%funcname)
                    token = self.useableMethods[funcname](self.detokenize(args))
                new_tokens.append(token)
            for i in range(len(new_tokens)):
                if isinstance(new_tokens[i], list) and len(new_tokens[i])>60:
                    ret = []
                    items = new_tokens[i]
                    for start in range(0,len(items),50):
                        ret.append(toMongoObj(new_tokens[:i]+[items[start:start+50]]+new_tokens[i+1:]))
                    return ret
            return [toMongoObj(new_tokens)]

    @expose('Prints this message')
    def help(self, args):
        sys.stderr.write( 'usage qt <command> [<args>]\n')
        sys.stderr.write( '\nThe qt commands are:\n')
        for method in self.methods:
            if method in self.methodMap:
                sys.stderr.write(' %-15s %s\n'%(method, getattr(self,self.methodMap[method]).description))
            else:
                sys.stderr.write(' %-15s %s\n'%(method, getattr(self,method).description))
        sys.exit(2)
        sys.exit(2)


    def run(self, args):
        method = args[0]
        args = args[1:]
        if method not in self.methods:
            sys.stderr.write('''qt: '%s' is not a jamo command.  Run 'jamo help' for more options\n'''%method)
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes)>0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n'%meth)
            sys.exit(64)
        if len(args)>2 and args[-2]=='-f' and os.path.isfile(args[-1]):
            file_name = args[-1]
            args = args[:-2]
            new_args = []
            with open(file_name) as fi:
                for line in fi.readlines():
                    new_args.append(line.rstrip())
            if len(new_args)>0:
                if len(new_args)>50:
                    for start in range(0,len(new_args),50):
                        getattr(self,method)(args+new_args[start:start+50])
                    return
                else:
                    args.extend(new_args)
        if method in self.methodMap:
            method = self.methodMap[method]
        getattr(self,method)(args)


def main():
    args = sys.argv[1:]
    qt = QT()
    if len(args)==0:
        qt.help(args)
    else:
        qt.run(args)

if __name__ == '__main__':
     main()
