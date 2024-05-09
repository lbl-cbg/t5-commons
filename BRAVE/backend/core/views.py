from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
#import requests
import urllib
import json

def get_species_list(request):
    content = [
        {"taxon_id": 11021, "species":'Eastern equine encephalitis virus'}, 
        {"taxon_id": 11036,"species":'Venezuelan equine encephalitis virus'},
        {"taxon_id": 37124,"species":'Chikungunya virus'}
    ]
    return JsonResponse(content, safe=False)


def get_species(request,taxonid):
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


def get_target(request,taxonid,braveid):

    login_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apilogin.aspx'
    braveapi_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apibrave.aspx'

    # the below is a token for Yeongshnn (generated by Gyorgy)
    api_key = 'sglims_8e5b950c-5205-4768-846d-6c9e267ce6a1'
    '''
    response = requests.get(login_endpoint, headers={'Accept': 'application/json', 'authorization': 'Bearer ' + api_key})
    print(response.text)
    response_json = response.json()
    sessionid = response_json['data']['sessionid']
    print(sessionid)
    '''
    print("============")
   
    req = urllib.request.Request(login_endpoint, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11','Accept':'application/json', 'authorization':'Bearer ' + api_key})
    #req.add_header('Accept', 'application/json')
    #req.add_header('authorization', 'Bearer ' + api_key)
    response = urllib.request.urlopen(req)
    content = response.read().decode(response.info().get_param('charset') or 'utf-8')
    response_json = json.loads(content)
    print(content) 
    sessionid = response_json['data']['sessionid']
    print(sessionid) 




    print("JIRE======>")
    jira_verifi_token_url = 'https://api.atlassian.com/jsm/assets/v1/imports/info' 
    jira_token = 'ATCTT3xFfGN0HnaSVsmK8gu3OGq-kUsxz2-DVJEy5Tc-d6uzdN7OAYqftKfzoArQJOHir1zR7FiXs1UHoavE6SwFSASc_eliIfDtx9V229Yu5qHU9gP6VojKM_7dnROwX1dy0Re2bdT-hlnMpjhSIXripOdgnrG1zTGhPxhDHIatydEsyzIjpnU=C04DBF1F'
    req = urllib.request.Request(jira_verifi_token_url, 
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                                          'Accept':'application/json', 
                                          'authorization':'Bearer ' + jira_token}) 
    response = urllib.request.urlopen(req)
    content = response.read().decode(response.info().get_param('charset') or 'utf-8')
    print(content) 
    jira_endpoints = json.loads(content)

    req = urllib.request.Request(jira_endpoints["links"]["getStatus"], 
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                                          'Accept':'application/json', 
                                          'authorization':'Bearer ' + jira_token}) 
    response = urllib.request.urlopen(req)
    content = response.read().decode(response.info().get_param('charset') or 'utf-8')
    print(content) 

    print("JIRE<======")


    return JsonResponse(content, safe=False)

    '''
    content = {
        "brave_id":'EEEV_MT',
        "af2_id":"Target_YP_9138111_103_261_ad6dd",
        "proteinseq":"SDVTDKCIASKAADLLTVMSTPDAETPSLCMHTDSTCRYHGSVAVYQDVYAVHAPTSIYYQALKGVRTIYWIGFDTTPFMYKNMAGAYPTYNTNWADESVLEARNIGLGSSDLHEKSFGKVSIMRKKKLQPTNKVIFSVGSTIYTEERILLRSWHLPNV",
        "twist_dnaseq":"TCTGACGTGACAGATAAGTGTATTGCGTCCAAAGCAGCCGATCTGCTGACAGTTATGTCTACACCTGATGCCGAAACGCCAAGCCTTTGTATGCATACCGACTCTACCTGCCGTTATCATGGCTCTGTTGCAGTTTATCAGGACGTATACGCTGTTCACGCACCAACCAGTATTTATTACCAGGCCCTTAAAGGTGTAAGAACAATCTATTGGATTGGCTTTGACACGACGCCTTTCATGTATAAGAACATGGCAGGCGCATATCCAACGTATAACACTAATTGGGCCGATGAATCTGTGCTGGAAGCGCGCAATATCGGTCTTGGATCTAGCGACCTGCACGAGAAATCCTTTGGCAAGGTTTCGATCATGCGTAAGAAAAAATTGCAGCCGACTAACAAGGTCATTTTTAGTGTCGGGAGCACCATCTACACCGAAGAACGCATCCTTCTTCGGTCTTGGCACTTACCGAATGTC"
    }
    return JsonResponse(content, safe=False)
    '''