from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
import requests
import urllib
import json
from django.conf import settings
import base64 
import re
import os

ANL_TOKEN = os.environ.get("ANL_API_TOKEN")
login_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apilogin.aspx'
braveapi_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apibrave.aspx'

def get_ANL_sessionid():
    # the below is a token for Yeongshnn (generated by Gyorgy)
    api_key = ANL_TOKEN
    response = requests.get(login_endpoint, headers={'Accept': 'application/json', 'authorization': 'Bearer ' + api_key})
    response_json = response.json()
    sessionid = None
    if 'data' in response_json:
        # get sessionid
        sessionid = response_json['data']['sessionid']
    else:
        print ('Could not get sessionid')

    # print sessionid
    print('\n', 'sessionid:' , sessionid)
    return sessionid


def get_species_list(request):
    content = [
        {"taxon_id": 37124,"species":'Chikungunya virus'},
        {"taxon_id": 11021, "species":'Eastern equine encephalitis virus'}, 
        {"taxon_id": 2697049,"species":'Wuhan seafood market pneumonia virus'},
        {"taxon_id": 11036,"species":'Venezuelan equine encephalitis virus'},
    ]
    return JsonResponse(content, safe=False)


def get_species(request,taxonid):
     
    sessionid = get_ANL_sessionid() 
    if not sessionid:
        return JsonResponse({'status':'false','message':'No session'}, status=500)
    
    # the possible actions for now: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\', \'crystalsummary\']
    # assemble JSON object
    print("Get Targets...")
    json_to_get_targets = '{"data" : [{"apiaction" : "targetsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
    response = requests.post(braveapi_endpoint, data = json_to_get_targets) 
    #print("rt:",response.text)
    targets_json = response.json()
    target_list = get_target_list_from_json(taxonid, targets_json["targets"])

    json_to_get_construct = '{"data" : [{"apiaction" : "constructsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
    response = requests.post(braveapi_endpoint, data = json_to_get_construct) 
    constructs_json = response.json()
    add_construct_data_to_target_list(target_list, constructs_json["constructs"])

    json_to_get_construct = '{"data" : [{"apiaction" : "purifiedproteinsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
    response = requests.post(braveapi_endpoint, data = json_to_get_construct) 
    purifiedproteinsummarys_json = response.json()
    add_purifiedproteins_data_to_target_list(target_list, purifiedproteinsummarys_json["purifiedproteins"])

    return JsonResponse(target_list, safe=False)

    '''
    content = {
        '11021':{
            "species":'Eastern equine encephalitis virus',
            "targets":[
                {
                    "target_id":"IDP99245",
                    "brave_id":"EEEV_HEL",
                    "anl_internal_id":"YP_913812.1"
                },
                {
                    "target_id":"IDP99245",
                    "brave_id":"EEEV_PEPT",
                    "anl_internal_id":"YP_913812.1"
                },
                {
                    "target_id":"IDP100401",
                    "brave_id":"EEEV_MT",
                    "anl_internal_id":"YP_913811.1"
                },
                {
                    "target_id":"IDP100402",
                    "brave_id":"EEEV_MACRO",
                    "anl_internal_id":"YP_913813.1"
                },
                {
                    "target_id":"IDP100403",
                    "brave_id":"EEEV_RDRP",
                    "anl_internal_id":"NP_740652.1"
                },
                {
                    "target_id":"IDP100419",
                    "brave_id":"EEEV_HEL_PEPT",
                    "anl_internal_id":"YP_913812.1"
                },
            ],
            "target_summary":[
                {
                    "target_id":"IDP99245",
                    "entries":3,
                    "pico":20000,
                    "designs":2,
                    "ni":30,
                    "constructs":2,
                    "exp":100,
                    "sol":20,
                    "cells":7,
                    "conc":24.14,
                    "purifier":"ctesar",
                    "yield":4,
                    "crystals":None,
                    "crystallographer":None,
                    "distribution":None,
                    "resolution":None,
                    "pdb_id":None
                }, 
                {
                    "target_id":"IDP100401",
                    "entries":2,
                    "pico":20000,
                    "designs":1,
                    "ni":0,
                    "constructs":1,
                    "exp":None,
                    "sol":None,
                    "cells":None,
                    "conc":None,
                    "purifier":None,
                    "yield":None,
                    "crystals":None,
                    "crystallographer":None,
                    "distribution":None,
                    "resolution":None,
                    "pdb_id":None
                }, 
                {
                    "target_id":"IDP100402",
                    "entries":2,
                    "pico":20000,
                    "designs":1,
                    "ni":70,
                    "constructs":1,
                    "exp":70,
                    "sol":70,
                    "cells":2.5,
                    "conc":36,
                    "purifier":"Istols",
                    "yield":54,
                    "crystals":16,
                    "crystallographer":"cchang",
                    "distribution":"2023-12-04",
                    "resolution":1.25,
                    "pdb_id":None
                },
                {
                    "target_id":"IDP100403",
                    "entries":2,
                    "pico":20000,
                    "designs":1,
                    "ni":50,
                    "constructs":1,
                    "exp":-1,
                    "sol":-1,
                    "cells":2.5,
                    "conc":13.6,
                    "purifier":"nmaltseva",
                    "yield":27,
                    "crystals":None,
                    "crystallographer":None,
                    "distribution":None,
                    "resolution":None,
                    "pdb_id":None
                },
                {
                    "target_id":"IDP100419",
                    "entries":2,
                    "pico":20000,
                    "designs":1,
                    "ni":0,
                    "constructs":1,
                    "exp":None,
                    "sol":None,
                    "cells":None,
                    "conc":None,
                    "purifier":None,
                    "yield":None,
                    "crystals":None,
                    "crystallographer":None,
                    "distribution":None,
                    "resolution":None,
                    "pdb_id":None
                },
            ]
        },
        '11036':{
            "species":'Venezuelan equine encephalitis virus',
            "targets":[
                {
                    "target_id":"IDP100404",
                    "brave_id":"VEEV_MT",
                    "anl_internal_id":"YP_010806451.1"
                },
                {
                    "target_id":"IDP100406",
                    "brave_id":"VEEV_HEL",
                    "anl_internal_id":"YP_010806452.1"
                },
                {
                    "target_id":"IDP100406",
                    "brave_id":"VEEV_PEPT",
                    "anl_internal_id":"YP_010806452.1"
                },
                {
                    "target_id":"IDP100407",
                    "brave_id":"VEEV_MACRO",
                    "anl_internal_id":"YP_010806453.1"
                },
                {
                    "target_id":"IDP100408",
                    "brave_id":"VEEV_RDRP",
                    "anl_internal_id":"YP_010806454.1"
                },
            ]
        }
    }

    return JsonResponse(content[taxonid], safe=False)
    '''


def get_target(request,taxonid,braveid):
    
    sessionid = get_ANL_sessionid() 
    if not sessionid:
        return JsonResponse({'status':'false','message':'No session'}, status=500)
    
    print("Get Targets...")
    json_to_get_targets = '{"data" : [{"apiaction" : "targetsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
    response = requests.post(braveapi_endpoint, data = json_to_get_targets) 
    targets_json = response.json()
    target_data = get_target_from_json(braveid, targets_json["targets"])

    json_to_get_construct = '{"data" : [{"apiaction" : "constructsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
    response = requests.post(braveapi_endpoint, data = json_to_get_construct) 
    constructs_json = response.json()
    target_data = add_construct_data_to_target_data(target_data, constructs_json["constructs"])

    return JsonResponse(target_data, safe=False)

    '''
    content = {
        "brave_id":'EEEV_MT',
        "af2_id":"Target_YP_9138111_103_261_ad6dd",
        "proteinseq":"SDVTDKCIASKAADLLTVMSTPDAETPSLCMHTDSTCRYHGSVAVYQDVYAVHAPTSIYYQALKGVRTIYWIGFDTTPFMYKNMAGAYPTYNTNWADESVLEARNIGLGSSDLHEKSFGKVSIMRKKKLQPTNKVIFSVGSTIYTEERILLRSWHLPNV",
        "twist_dnaseq":"TCTGACGTGACAGATAAGTGTATTGCGTCCAAAGCAGCCGATCTGCTGACAGTTATGTCTACACCTGATGCCGAAACGCCAAGCCTTTGTATGCATACCGACTCTACCTGCCGTTATCATGGCTCTGTTGCAGTTTATCAGGACGTATACGCTGTTCACGCACCAACCAGTATTTATTACCAGGCCCTTAAAGGTGTAAGAACAATCTATTGGATTGGCTTTGACACGACGCCTTTCATGTATAAGAACATGGCAGGCGCATATCCAACGTATAACACTAATTGGGCCGATGAATCTGTGCTGGAAGCGCGCAATATCGGTCTTGGATCTAGCGACCTGCACGAGAAATCCTTTGGCAAGGTTTCGATCATGCGTAAGAAAAAATTGCAGCCGACTAACAAGGTCATTTTTAGTGTCGGGAGCACCATCTACACCGAAGAACGCATCCTTCTTCGGTCTTGGCACTTACCGAATGTC"
    }
    return JsonResponse(content, safe=False)
    '''



def get_target_list_from_json(taxonid, data_list):
    out_list = []
    targetid_list = {}
    for item in data_list:
        if item["taxonid"]==taxonid:
            if item["targetid"] not in targetid_list:
                targetid_list[item["targetid"]]=True
                out_list.append(item)
    return out_list


def get_target_from_json(originaltargetid, data_list): 
    for item in data_list:
        if item["originaltargetid"]==originaltargetid:
            return item
    return None


def add_construct_data_to_target_list(target_list, constructs):
    for titem in target_list:
        for citem in constructs:
            if citem["targetid"]==titem["targetid"]:
                titem["vector"] = citem.get("vector", "null")
                titem["expressionlevel"] = citem.get("expressionlevel", "null")
                titem["expressionlevelunit"] = citem.get("expressionlevelunit", "null")
    return target_list

def add_construct_data_to_target_data(target_data, constructs):
    
    for citem in constructs:
        if citem["targetid"]==target_data["targetid"]:
            target_data.update(citem)
            #target_data["targetproteinsequence"] = citem.get("targetproteinsequence", "null") 
    return target_data


def add_purifiedproteins_data_to_target_list(target_list, purifiedproteins):
    for titem in target_list:
        for citem in purifiedproteins:
            if citem["targetid"]==titem["targetid"]:
                titem["proteinconcentration"] = citem.get("proteinconcentration", "null")
                titem["proteinconcentrationunit"] = citem.get("proteinconcentrationunit", "null")
                titem["proteinvolume"] = citem.get("proteinvolume", "null")
                titem["proteinvolumeunit"] = citem.get("proteinvolumeunit", "null")
                titem["buffercontent"] = citem.get("buffercontent", "null")
    return target_list