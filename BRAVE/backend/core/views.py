from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
import requests
import urllib
import json
from django.conf import settings
import base64 
import re


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
    content = {
        "brave_id":'EEEV_MT',
        "af2_id":"Target_YP_9138111_103_261_ad6dd",
        "proteinseq":"SDVTDKCIASKAADLLTVMSTPDAETPSLCMHTDSTCRYHGSVAVYQDVYAVHAPTSIYYQALKGVRTIYWIGFDTTPFMYKNMAGAYPTYNTNWADESVLEARNIGLGSSDLHEKSFGKVSIMRKKKLQPTNKVIFSVGSTIYTEERILLRSWHLPNV",
        "twist_dnaseq":"TCTGACGTGACAGATAAGTGTATTGCGTCCAAAGCAGCCGATCTGCTGACAGTTATGTCTACACCTGATGCCGAAACGCCAAGCCTTTGTATGCATACCGACTCTACCTGCCGTTATCATGGCTCTGTTGCAGTTTATCAGGACGTATACGCTGTTCACGCACCAACCAGTATTTATTACCAGGCCCTTAAAGGTGTAAGAACAATCTATTGGATTGGCTTTGACACGACGCCTTTCATGTATAAGAACATGGCAGGCGCATATCCAACGTATAACACTAATTGGGCCGATGAATCTGTGCTGGAAGCGCGCAATATCGGTCTTGGATCTAGCGACCTGCACGAGAAATCCTTTGGCAAGGTTTCGATCATGCGTAAGAAAAAATTGCAGCCGACTAACAAGGTCATTTTTAGTGTCGGGAGCACCATCTACACCGAAGAACGCATCCTTCTTCGGTCTTGGCACTTACCGAATGTC"
    }
    return JsonResponse(content, safe=False)
    