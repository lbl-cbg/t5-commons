#!/usr/bin/env python2
import argparse
import sdm_curl
import sdm_common
import json
import os
import sys
import subprocess
import tempfile

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line utiltity to manage JAMO search services/datastores')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s','--service-name',dest='service',type=str, help='name of the service')
    group.add_argument('-n','--store-name',dest='store',type=str, help='key to the data store')
    parser.add_argument('-d','--data-file',dest='data_file',type=str,help='Json data file to post')
    parser.add_argument('-a',"--host", type=str, help="The server address of sdm",default='https://jamo.jgi.doe.gov')

    args = parser.parse_args()
    if args.store is None and args.service is None:
        parser.print_help()
        sys.exit(0)
    token = sdm_common.getToken(args.host)
    if token is None:
        print 'Sorry no token found :('
        sys.exit(1)

    sdm = sdm_curl.Curl(args.host, appToken=token,retry=0)

    def edit(jsonO):
        if isinstance(jsonO, str):
            string = jsonO
        else:
            string = json.dumps(jsonO,indent=4, separators=(',', ':'))
        f, fname = tempfile.mkstemp()
        f = open(fname,'w')
        f.write(string)
        f.close()
        cmd = os.environ.get('EDITOR', 'vi') + ' ' + fname
        subprocess.call(cmd, shell=True)
        with open(fname, 'r') as f:
            ret = f.read()  
        os.unlink(fname)
        try:
            return json.loads(ret)
        except:
            value = raw_input("Your json was invalid would you like to try again? [Y/n]")
            if value.lower()=='n':
                sys.exit(1)
            else:
                return edit(ret)
    
    if args.service is not None:
        try:
            serviceJson = sdm.get('api/metadata/service/%s'%args.service)
            del serviceJson['_id']
            del serviceJson['owner']
        except sdm_curl.CurlHttpException as e:
            if e.code==404:
                serviceJson = {'name':args.service,'return':['_id','file_name'],'query':{}}
            else:
                raise e
            
        newServiceJson = edit(serviceJson)
        sdm.post('api/metadata/service',data=newServiceJson)

    elif args.store is not None:
        try:
            storeJson = sdm.get('api/metadata/datastore/%s'%args.store)
            del storeJson['_id']
            if 'owner' in storeJson:
                del storeJson['owner']
        except sdm_curl.CurlHttpException as e:
            if e.code==404:
                storeJson = {'key':args.store,'url':'','identifier':'','map':{}}
            else:
                raise e

        newStoreJson = edit(storeJson)
        sdm.post('api/metadata/datastore',data=newStoreJson)

