import requests 
import json
#from django.conf import settings
import base64 
#import re
import os
from .jira_import_util import JIRAImportUtil, JIRAObjectID
from urllib.parse import quote

class CSBIDTask(JIRAImportUtil):    

    ANL_TOKEN = os.environ.get("ANL_API_TOKEN")
    JIRA_API_TOKEN_YONG = os.environ.get("JIRA_API_TOKEN_YONG")
    username = "yong@lbl.gov"
    credentials = f'{username}:{JIRA_API_TOKEN_YONG}'
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8') 
    jira_servicedeskapi_url = 'https://taskforce5.atlassian.net/rest/servicedeskapi/assets/workspace' 

    #map species name to jira virus object id
    virus_map = {"chikungunya virus":411,
                "eastern equine encephalitis virus":410,
                "venezuelan equine encephalitis virus":409,
                "wuhan seafood market pneumonia virus":764}
    target_id_map = {}
    all_pur_protein_id_map = {}
    pur_protein_id_map = {}


    def __init__(self):
        self.workspace_id = self.get_workspace_id()
        self.base_url = self.get_base_url(self.workspace_id) 


    def get_workspace_id(self):
        response = requests.get(self.jira_servicedeskapi_url, 
                                headers={'Content-Type': 'application/json', 
                                        'authorization': 'Basic ' + self.encoded_credentials})
        workspaces_response_json = response.json() 
        print("JIRA workspaces:", workspaces_response_json)
        return workspaces_response_json["values"][0]["workspaceId"]
 

    def get_base_url(self, workspace_id):
        return f'https://api.atlassian.com/jsm/assets/workspace/{workspace_id}/v1'
 

    def run(self):
        json_data = self.get_data()   
         
        
        
        targets_json = json_data["targets_json"]["targets"]
        #targets_json = list(filter(lambda x: x["targetannotation"]!="", targets_json)) #remove empty targetannotation
        #build attributes_data_list ,a list of attributes data/payload
        attributes_data_list = []
        for data_item in targets_json:
            attributes_data = self.get_target_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.TARGET.value,
                                    targets_json,
                                    attributes_data_list,
                                    "Target",
                                    "Target ID", 
                                    "targetid") 
        self.setTargetIDMap(create_record_response)
        
        constructs_json = json_data["constructs_json"]["constructs"]  
        attributes_data_list = []
        for data_item in constructs_json:
            attributes_data = self.get_construct_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.CONSTRUCT.value,
                                    constructs_json,
                                    attributes_data_list,
                                    "Construct",
                                    "Target Trexp ID", 
                                    ["targetid","trexpid"])
        
        purifiedproteins_json = json_data["purifiedproteins_json"] 
        attributes_data_list = []
        for data_item in purifiedproteins_json["purifiedproteins"]:
            attributes_data = self.get_purifiedproteins_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.PURIFIED_PROTEINS.value,
                                    purifiedproteins_json["purifiedproteins"],
                                    attributes_data_list,
                                    "Purified Protein",
                                    "Purbatchcproprcid", 
                                    "purbatchcproprcid")
        self.setPurProteinIDMap(create_record_response, "326")
        attributes_data_list = []
        for data_item in purifiedproteins_json["proteininventory"]:
            attributes_data = self.get_proteininventory_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.PROTEIN_INVENTORY.value,
                                    purifiedproteins_json["proteininventory"],
                                    attributes_data_list,
                                    "Protein Inventory",
                                    "Purbatchcproprcid", 
                                    "purbatchcproprcid")
        
        all_purifiedproteins_json = json_data["all_purifiedproteins_json"]["allpurifiedproteins"] 
        all_purifiedproteins_json = self.remove_purifiedproteins_from_all_purifiedproteins(json_data["purifiedproteins_json"]["purifiedproteins"], all_purifiedproteins_json)
        attributes_data_list = []
        for data_item in all_purifiedproteins_json:
            attributes_data = self.get_all_purifiedproteins_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.All_PURIFIED_PROTEINS.value,
                                    all_purifiedproteins_json,
                                    attributes_data_list,
                                    "All Purified Proteins",
                                    "Purbatchcproprcid", 
                                    "purbatchcproprcid")
        self.setAllPurProteinIDMap(create_record_response, "374")
         
        allcrystalsummary_json = json_data["allcrystalsummary_json"]["allcrystals"] #[0:50] #TODO, TEST ONLY REMOVE [0:50]  
        attributes_data_list = []
        for data_item in allcrystalsummary_json:
            attributes_data = self.get_allcrystalsummary_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.ALL_CRYSTAL_SUMMARY.value,
                                    allcrystalsummary_json,
                                    attributes_data_list,
                                    "All Crystal Summary",
                                    "Crystallization ID", 
                                    "crystallizationid") 
        '''
        crystals_json = json_data["crystals_json"]["crystals"]
        attributes_data_list = []
        for data_item in crystals_json:
            attributes_data = self.get_crystal_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.CRYSTAL.value,
                                    crystals_json,
                                    attributes_data_list,
                                    "Crystal",
                                    "Crystallization Harvest ID", 
                                    ["crystallizationid","harvestid"])      

        for structure_json in json_data["structuresummary_json_list"]: 
            attributes_data_list = []
            for data_item in structure_json:
                attributes_data = self.get_structuresummary_attributes_data(data_item) 
                attributes_data_list.append(attributes_data) 
            create_record_response = super().create_new_jira_record(
                                        JIRAObjectID.STRUCTURE.value,
                                        structure_json,
                                        attributes_data_list,
                                        "Structure",
                                        "Harvest ID", 
                                        "harvestid")   
            if len(create_record_response) == len(structure_json):
                for index,ritem in enumerate(create_record_response):
                    self.upload_attachment(ritem["id"], structure_json[index])  
            else:
                print("Something wrong in push structures, len(create_record_response) not equal len(structure_json)")
        '''
        print("Total targets from api:",len(targets_json)) 
        print("Total constructs from api:",len(constructs_json)) 
        print("Total purified proteins from api:",len(purifiedproteins_json["purifiedproteins"]))
        print("Total protein inventory from api:",len(purifiedproteins_json["proteininventory"]))
        print("Total all purifiedproteins from api:",len(all_purifiedproteins_json))
        print("Total allcrystalsummary from api:",len(allcrystalsummary_json))
        #print("Total crystals from api:",len(crystals_json)) 

        return create_record_response
    

    def get_data(self):
        print('Get data...') 
        login_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apilogin.aspx'
        braveapi_endpoint = 'https://sg.bio.anl.gov/intranet/utilities/servers/apibrave.aspx'
        
        # the below is a token for Yeongshnn (generated by Gyorgy)
        api_key = self.ANL_TOKEN
        response = requests.get(login_endpoint, headers={'Accept': 'application/json', 'authorization': 'Bearer ' + api_key})
        response_json = response.json()

        sessionid = ''

        if 'data' in response_json:
            # get sessionid
            sessionid = response_json['data']['sessionid']

        else:
            print ('Could not get sessionid')

        # print sessionid
        print('\n', 'sessionid:' , sessionid)
        
        # the possible actions for now: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\', \'crystalsummary\']
        # assemble JSON object
        print("Get Targets...")
        json_to_get_targets = '{"data" : [{"apiaction" : "targetsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_targets)
        #print(response.text)
        targets_json = response.json()
        #return targets_json

        print("Get Constructs...")
        json_to_get_constructs = '{"data" : [{"apiaction" : "constructsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_constructs)
        constructs_json = response.json()
        
        print("Get Purified Proteins...")
        json_to_get_purifiedproteins = '{"data" : [{"apiaction" : "purifiedproteinsummary", "metadata" :"possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_purifiedproteins)
        purifiedproteins_json = response.json()
 
        print("Get All Purified Proteins...")
        json_to_get_all_purifiedproteins = '{"data" : [{"apiaction" : "allpurifiedproteinsummary", "metadata" :"possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_all_purifiedproteins)
        all_purifiedproteins_json = response.json()

        print("Get All Crystal Summary...")
        json_to_get_allcrystalsummary = '{"data" : [{"apiaction" : "allcrystalsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_allcrystalsummary)
        allcrystalsummary_json = response.json()
        print("All Crystal Summary source size:", len(allcrystalsummary_json["allcrystals"]))

        #============REMOVE
        for item in purifiedproteins_json["purifiedproteins"]:
            if item["purbatchcproprcid"]=="30884":
                print("p:", item)
        the_item = None 
        for item in allcrystalsummary_json["allcrystals"]:
            if item["crystallizationid"]=="102464":
                the_item = item
                print("ac:", item)
        allcrystalsummary_json["allcrystals"] = [the_item]
        #============REMOVE

        
        print("Get Crystal Summary...")
        json_to_get_crystalsummary = '{"data" : [{"apiaction" : "crystalsummary", "metadata" : "possible apiactions: [\'targetsummary\', \'constructsummary\', \'purifiedproteinsummary\', \'allpurifiedproteinsummary\', \'allcrystalsummary\' , \'crystalsummary\']"}],"submissionid" : "' + sessionid +'"}'
        response = requests.post(braveapi_endpoint, data = json_to_get_crystalsummary)
        crystals_json = response.json()
        print("Crystal Summary source size:", len(crystals_json["crystals"]))

        print("Get Structure...") 
        crystallizationid_harvestid_map = {}
        structuresummary_json_list=[]
        for citem in crystals_json["crystals"]:    
            if (citem["crystallizationid"]+citem["harvestid"]) not in crystallizationid_harvestid_map:
                print(".", end="\r")#print("c harvestid:",citem["crystallizationid"],citem["harvestid"])
                crystallizationid_harvestid_map[citem["crystallizationid"]+citem["harvestid"]] = True
                json_to_get_structuresummary = '{"data" : [{"apiaction" : "structuresummary", "metadata" : "harvestid=' + citem["harvestid"] +'"}],"submissionid" : "' + sessionid +'"}'
                response = requests.post(braveapi_endpoint, data = json_to_get_structuresummary)
                structuresummary_json = response.json()
                if 'crystals' in structuresummary_json: 
                    structuresummary_json_list.append(structuresummary_json["crystals"])

        return {"targets_json":targets_json,
                "constructs_json":constructs_json,
                "purifiedproteins_json":purifiedproteins_json,
                "all_purifiedproteins_json":all_purifiedproteins_json,
                "allcrystalsummary_json":allcrystalsummary_json,
                "crystals_json":crystals_json,
                "structuresummary_json_list":structuresummary_json_list}
    


    def get_target_attributes_data(self, data):
        return [
            {
                    "objectTypeAttributeId": "96",  # Attribute ID for Targetid
                    "objectAttributeValues": [
                        {
                            "value": data["targetid"].strip()  #Targetid    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "97", #Originaltargetid
                    "objectAttributeValues": [
                        {
                            "value": data["originaltargetid"].strip()  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "491", #Parent Name
                    "objectAttributeValues": [
                        {
                            "value": self.getVirusObjectID(data["speciesname"].strip()) 
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "99", #Taxonid
                    "objectAttributeValues": [
                        {
                            "value": data["taxonid"].strip() 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "100", #Targetannotation , unique key
                    "objectAttributeValues": [
                        {
                            "value": data["targetannotation"].strip() 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "101", #Parentproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseguid"].strip()  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "102", #Parentproteinseq
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseq"].strip()  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "103", #Parentdnaseq
                    "objectAttributeValues": [
                        {
                            "value": data["parentdnaseq"].strip()  
                        }
                    ]
            }
        ]

    
    def get_construct_attributes_data(self, data):
        return [
            {
                    "objectTypeAttributeId": "107",  # Cloneidf
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "493",  # Target Annotation
                    "objectAttributeValues": [
                        {
                            "value": data["targetannotation"].strip()   
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "492", #Parent Virus
                    "objectAttributeValues": [
                        {
                            "value": self.getVirusObjectID(data["speciesname"].strip()) 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "104",  # Designid
                    "objectAttributeValues": [
                        {
                            "value": data["designid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "105",  # Targetid
                    "objectAttributeValues": [
                        {
                            "value": self.getTargetObjectID(data["targetid"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "106",  # Originaltargetid
                    "objectAttributeValues": [
                        {
                            "value": data["originaltargetid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "108",  # Oligoplate
                    "objectAttributeValues": [
                        {
                            "value": data["oligoplate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "109",  # Platewell
                    "objectAttributeValues": [
                        {
                            "value": data["platewell"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "110",  # Pcryield
                    "objectAttributeValues": [
                        {
                            "value": data["pcryield"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "111",  # Trexpid
                    "objectAttributeValues": [
                        {
                            "value": data["trexpid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "112",  # Expupdated
                    "objectAttributeValues": [
                        {
                            "value": data["expupdated"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "113",  # Vector
                    "objectAttributeValues": [
                        {
                            "value": data["vector"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "114",  # Growthtemperature
                    "objectAttributeValues": [
                        {
                            "value": data["growthtemperature"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "115",  # Inductiontemperature
                    "objectAttributeValues": [
                        {
                            "value": data["inductiontemperature"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "116",  # Inductionreagent
                    "objectAttributeValues": [
                        {
                            "value": data["inductionreagent"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "117",  # Expressionlevel
                    "objectAttributeValues": [
                        {
                            "value": data["expressionlevel"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "118",  # Imaclevel
                    "objectAttributeValues": [
                        {
                            "value": data["imaclevel"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "119",  # Distributionbit
                    "objectAttributeValues": [
                        {
                            "value": data["distributionbit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "120",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "121",  # Targetproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "122",  # Parentproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseguid"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "125",  # Targetproteinsequence
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinsequence"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "126",  # Codingsequence
                    "objectAttributeValues": [
                        {
                            "value": data["codingsequence"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "533",  # Target Trexp ID(unique id)
                    "objectAttributeValues": [
                        {
                            "value": data["targetid"]+"_"+data["trexpid"]  
                        }
                    ]
            }, 
        ]

    
    def get_purifiedproteins_attributes_data(self, data):
        return [ 
            {
                    "objectTypeAttributeId": "325",  # Cloneidf
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "529",  # Parent Virus
                    "objectAttributeValues": [
                        {
                            "value": self.getVirusObjectID(data["speciesname"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "530",  # Target Annotation
                    "objectAttributeValues": [
                        {
                            "value": data["targetannotation"].strip()
                        }
                    ]
            },  
            {
                    "objectTypeAttributeId": "323",  # Targetid
                    "objectAttributeValues": [
                        {
                            "value": self.getTargetObjectID(data["targetid"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "324",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "326",  # Purbatchcproprcid
                    "objectAttributeValues": [
                        {
                            "value": data["purbatchcproprcid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "327",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "328",  # Purificationdate
                    "objectAttributeValues": [
                        {
                            "value": data["purificationdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "329",  # Purificationworkflow
                    "objectAttributeValues": [
                        {
                            "value": data["purificationworkflow"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "330",  # Proteinconcentration
                    "objectAttributeValues": [
                        {
                            "value": data["proteinconcentration"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "331",  # Proteinconcentrationunit
                    "objectAttributeValues": [
                        {
                            "value": data["proteinconcentrationunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "332",  # Proteinvolume
                    "objectAttributeValues": [
                        {
                            "value": data["proteinvolume"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "333",  # Proteinvolumeunit
                    "objectAttributeValues": [
                        {
                            "value": data["proteinvolumeunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "334",  # Buffername
                    "objectAttributeValues": [
                        {
                            "value": data["buffername"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "335",  # Buffercontent
                    "objectAttributeValues": [
                        {
                            "value": data["buffercontent"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "336",  # Targetproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "337",  # Parentproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseguid"]  
                        }
                    ]
            }, 
            
            {
                    "objectTypeAttributeId": "340",  # targetproteinsequence
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinsequence"]  
                        }
                    ]
            } 
        ]


    def get_proteininventory_attributes_data(self, data):
        return [ 
            {
                    "objectTypeAttributeId": "347",  # Cloneidf
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "527",  # Parent Virus
                    "objectAttributeValues": [
                        {
                            "value": self.getVirusObjectID(data["speciesname"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "528",  # Target Annotation
                    "objectAttributeValues": [
                        {
                            "value": data["targetannotation"].strip()   
                        }
                    ]
            },  
            {
                    "objectTypeAttributeId": "345",  # Targetid
                    "objectAttributeValues": [
                        {
                            "value": self.getTargetObjectID(data["targetid"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "346",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "348",  # Purbatchcproprcid
                    "objectAttributeValues": [
                        {
                            "value": data["purbatchcproprcid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "349",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "350",  # Purificationdate
                    "objectAttributeValues": [
                        {
                            "value": data["purificationdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "351",  # Purificationworkflow
                    "objectAttributeValues": [
                        {
                            "value": data["purificationworkflow"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "352",  # Buffername
                    "objectAttributeValues": [
                        {
                            "value": data["buffername"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "353",  # Buffercontent
                    "objectAttributeValues": [
                        {
                            "value": data["buffercontent"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "354",  # Targetproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "355",  # Parentproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "358",  # targetproteinsequence
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinsequence"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "359",  # Inventoryid
                    "objectAttributeValues": [
                        {
                            "value": data["inventoryid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "360",  # Currentvolume
                    "objectAttributeValues": [
                        {
                            "value": data["currentvolume"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "361",  # Currentvolumeunit
                    "objectAttributeValues": [
                        {
                            "value": data["currentvolumeunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "362",  # Currentconcentration
                    "objectAttributeValues": [
                        {
                            "value": data["currentconcentration"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "363",  # Currentconcentrationunit
                    "objectAttributeValues": [
                        {
                            "value": data["currentconcentrationunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "364",  # Storageinfo
                    "objectAttributeValues": [
                        {
                            "value": data["storageinfo"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "365",  # Inventorynotes
                    "objectAttributeValues": [
                        {
                            "value": data["inventorynotes"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "366",  # Shipments
                    "objectAttributeValues": [
                        {
                            "value": json.dumps(data["shipments"]) #TODO: might need parse  
                        }
                    ]
            }        
        ]


    def get_all_purifiedproteins_attributes_data(self, data):
        return [ 
            {
                    "objectTypeAttributeId": "371",  # Targetid
                    "objectAttributeValues": [
                        {
                            "value": self.getTargetObjectID(data["targetid"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "372",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "373",  # Clone IDF
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "525",  # Parent Virus
                    "objectAttributeValues": [
                        {
                            "value": self.getVirusObjectID(data["speciesname"].strip())   
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "526",  # Target Annotation
                    "objectAttributeValues": [
                        {
                            "value": data["targetannotation"].strip()
                        }
                    ]
            },  
            {
                    "objectTypeAttributeId": "374",  # Purbatchcproprcid
                    "objectAttributeValues": [
                        {
                            "value": data["purbatchcproprcid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "375",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "376",  # Purificationdate
                    "objectAttributeValues": [
                        {
                            "value": data["purificationdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "377",  # Purificationworkflow
                    "objectAttributeValues": [
                        {
                            "value": data["purificationworkflow"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "378",  # Proteinconcentration
                    "objectAttributeValues": [
                        {
                            "value": data["proteinconcentration"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "379",  # Proteinconcentrationunit
                    "objectAttributeValues": [
                        {
                            "value": data["proteinconcentrationunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "380",  # Proteinvolume
                    "objectAttributeValues": [
                        {
                            "value": data["proteinvolume"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "381",  # Proteinvolumeunit
                    "objectAttributeValues": [
                        {
                            "value": data["proteinvolumeunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "382",  # Buffername
                    "objectAttributeValues": [
                        {
                            "value": data["buffername"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "383",  # Buffercontent
                    "objectAttributeValues": [
                        {
                            "value": data["buffercontent"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "384",  # Covalentmodification
                    "objectAttributeValues": [
                        {
                            "value": data["covalentmodification"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "385",  # Limitedproteolysis
                    "objectAttributeValues": [
                        {
                            "value": data["limitedproteolysis"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "386",  # Ligands
                    "objectAttributeValues": [
                        {
                            "value": data["ligands"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "387",  # Targetproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "388",  # Parentproteinseguid
                    "objectAttributeValues": [
                        {
                            "value": data["parentproteinseguid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "391",  # targetproteinsequence
                    "objectAttributeValues": [
                        {
                            "value": data["targetproteinsequence"]  
                        }
                    ]
            } 
        ]
    

    def get_allcrystalsummary_attributes_data(self, data):
        out=[]
        out.append({
                    "objectTypeAttributeId": "396",  # Crystallizationid
                    "objectAttributeValues": [
                        {
                            "value": data["crystallizationid"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "397",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "398",  # Clone IDF
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"].strip()  
                        }
                    ]
            })
        
        v = self.getPurProteinObjectID(data["purbatchcproprcid"].strip())  
        print("pid:", data["purbatchcproprcid"].strip())
        print("m:",self.pur_protein_id_map)
        print("V:",v)
        if v != None:
            out.append({
                        "objectTypeAttributeId": "399",  # Purbatchcproprcid
                        "objectAttributeValues": [
                            {
                                "value": v
                            }
                        ]
                })
            
        out.append({
                    "objectTypeAttributeId": "400",  # Setupdate
                    "objectAttributeValues": [
                        {
                            "value": data["setupdate"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "401",  # Screenname
                    "objectAttributeValues": [
                        {
                            "value": data["screenname"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "402",  # Temperature
                    "objectAttributeValues": [
                        {
                            "value": data["temperature"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "403",  # Temperatureunit
                    "objectAttributeValues": [
                        {
                            "value": data["temperatureunit"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "404",  # Plate
                    "objectAttributeValues": [
                        {
                            "value": data["plate"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "405",  # Subwell
                    "objectAttributeValues": [
                        {
                            "value": data["subwell"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "406",  # Well
                    "objectAttributeValues": [
                        {
                            "value": data["well"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "407",  # Growthtime
                    "objectAttributeValues": [
                        {
                            "value": data["growthtime"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "408",  # Growthtimeunit
                    "objectAttributeValues": [
                        {
                            "value": data["growthtimeunit"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "409",  # Score 
                    "objectAttributeValues": [
                        {
                            "value": data["score"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "410",  # Modifications 
                    "objectAttributeValues": [
                        {
                            "value": data["modifications"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "411",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "412",  # Distributedto
                    "objectAttributeValues": [
                        {
                            "value": data["distributedto"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "413",  # Crystallographer
                    "objectAttributeValues": [
                        {
                            "value": data["crystallographer"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "414",  # Description
                    "objectAttributeValues": [
                        {
                            "value": data["description"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "415",  # Resolution
                    "objectAttributeValues": [
                        {
                            "value": data["resolution"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "416",  # Resolutionunit
                    "objectAttributeValues": [
                        {
                            "value": data["resolutionunit"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "417",  # Cryoname
                    "objectAttributeValues": [
                        {
                            "value": data["cryoname"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "418",  # Testdate
                    "objectAttributeValues": [
                        {
                            "value": data["testdate"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "419",  # Pdbid
                    "objectAttributeValues": [
                        {
                            "value": data["pdbid"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "420",  # Notes
                    "objectAttributeValues": [
                        {
                            "value": data["notes"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "421",  # Ligandname
                    "objectAttributeValues": [
                        {
                            "value": data["ligandname"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "422",  # Ligandtype
                    "objectAttributeValues": [
                        {
                            "value": data["ligandtype"]  
                        }
                    ]
            })
        out.append({
                    "objectTypeAttributeId": "423",  # Harvestid
                    "objectAttributeValues": [
                        {
                            "value": data["harvestid"]  
                        }
                    ]
            } ) 
        
        return out


    def get_crystal_attributes_data(self, data):
        return [ 
            {
                    "objectTypeAttributeId": "427",  # Crystallizationid
                    "objectAttributeValues": [
                        {
                            "value": data["crystallizationid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "428",  # Cloneid
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "532",  # Clone IDF
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"].strip()
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "430",  # Purbatchcproprcid
                    "objectAttributeValues": [
                        {
                            "value": self.getPurProteinObjectID(data["purbatchcproprcid"].strip())    
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "431",  # Setupdate
                    "objectAttributeValues": [
                        {
                            "value": data["setupdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "432",  # Screenname
                    "objectAttributeValues": [
                        {
                            "value": data["screenname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "433",  # Temperature
                    "objectAttributeValues": [
                        {
                            "value": data["temperature"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "434",  # Temperatureunit
                    "objectAttributeValues": [
                        {
                            "value": data["temperatureunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "435",  # Plate
                    "objectAttributeValues": [
                        {
                            "value": data["plate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "436",  # Subwell
                    "objectAttributeValues": [
                        {
                            "value": data["subwell"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "437",  # Well
                    "objectAttributeValues": [
                        {
                            "value": data["well"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "438",  # Growthtime
                    "objectAttributeValues": [
                        {
                            "value": data["growthtime"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "439",  # Growthtimeunit
                    "objectAttributeValues": [
                        {
                            "value": data["growthtimeunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "440",  # Score 
                    "objectAttributeValues": [
                        {
                            "value": data["score"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "441",  # Modifications 
                    "objectAttributeValues": [
                        {
                            "value": data["modifications"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "442",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "443",  # Distributedto
                    "objectAttributeValues": [
                        {
                            "value": data["distributedto"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "444",  # Crystallographer
                    "objectAttributeValues": [
                        {
                            "value": data["crystallographer"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "445",  # Description
                    "objectAttributeValues": [
                        {
                            "value": data["description"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "446",  # Resolution
                    "objectAttributeValues": [
                        {
                            "value": data["resolution"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "447",  # Resolutionunit
                    "objectAttributeValues": [
                        {
                            "value": data["resolutionunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "448",  # Cryoname
                    "objectAttributeValues": [
                        {
                            "value": data["cryoname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "449",  # Testdate
                    "objectAttributeValues": [
                        {
                            "value": data["testdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "450",  # Pdbid
                    "objectAttributeValues": [
                        {
                            "value": data["pdbid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "451",  # Notes
                    "objectAttributeValues": [
                        {
                            "value": data["notes"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "452",  # Ligandname
                    "objectAttributeValues": [
                        {
                            "value": data["ligandname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "453",  # Ligandtype
                    "objectAttributeValues": [
                        {
                            "value": data["ligandtype"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "454",  # Harvestid
                    "objectAttributeValues": [
                        {
                            "value": data["harvestid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "568",  # Crystallization Harvest ID
                    "objectAttributeValues": [
                        {
                            "value": data["crystallizationid"]+"_"+data["harvestid"]  
                        }
                    ]
            }  
        ]

    
    def get_structuresummary_attributes_data(self, data):
        return [ 
            {
                    "objectTypeAttributeId": "539",  # Crystallization ID
                    "objectAttributeValues": [
                        {
                            "value": data["crystallizationid"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "540",  # Clone ID
                    "objectAttributeValues": [
                        {
                            "value": data["cloneid"].strip()  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "541",  # Clone IDF
                    "objectAttributeValues": [
                        {
                            "value": data["cloneidf"].strip()
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "542",  # Purbatchcproprcid
                    "objectAttributeValues": [
                        {
                            "value": self.getPurProteinObjectID(data["purbatchcproprcid"].strip())  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "543",  # Setupdate
                    "objectAttributeValues": [
                        {
                            "value": data["setupdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "544",  # Screenname
                    "objectAttributeValues": [
                        {
                            "value": data["screenname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "545",  # Temperature
                    "objectAttributeValues": [
                        {
                            "value": data["temperature"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "546",  # Temperatureunit
                    "objectAttributeValues": [
                        {
                            "value": data["temperatureunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "547",  # Plate
                    "objectAttributeValues": [
                        {
                            "value": data["plate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "548",  # Subwell
                    "objectAttributeValues": [
                        {
                            "value": data["subwell"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "549",  # Well
                    "objectAttributeValues": [
                        {
                            "value": data["well"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "550",  # Growthtime
                    "objectAttributeValues": [
                        {
                            "value": data["growthtime"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "551",  # Growthtimeunit
                    "objectAttributeValues": [
                        {
                            "value": data["growthtimeunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "552",  # Score
                    "objectAttributeValues": [
                        {
                            "value": data["score"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "553",  # Modifications
                    "objectAttributeValues": [
                        {
                            "value": data["modifications"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "554",  # Purifier
                    "objectAttributeValues": [
                        {
                            "value": data["purifier"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "555",  # Distributedto
                    "objectAttributeValues": [
                        {
                            "value": data["distributedto"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "556",  # Crystallographer
                    "objectAttributeValues": [
                        {
                            "value": data["crystallographer"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "557",  # Description
                    "objectAttributeValues": [
                        {
                            "value": data["description"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "558",  # Resolution
                    "objectAttributeValues": [
                        {
                            "value": data["resolution"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "559",  # Resolutionunit
                    "objectAttributeValues": [
                        {
                            "value": data["resolutionunit"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "560",  # Cryoname
                    "objectAttributeValues": [
                        {
                            "value": data["cryoname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "561",  # Testdate
                    "objectAttributeValues": [
                        {
                            "value": data["testdate"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "562",  # Pdbid
                    "objectAttributeValues": [
                        {
                            "value": data["pdbid"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "563",  # Notes
                    "objectAttributeValues": [
                        {
                            "value": data["notes"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "564",  # Ligandname
                    "objectAttributeValues": [
                        {
                            "value": data["ligandname"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "565",  # Ligandtype
                    "objectAttributeValues": [
                        {
                            "value": data["ligandtype"]  
                        }
                    ]
            }, 
            {
                    "objectTypeAttributeId": "567",  # Harvest ID
                    "objectAttributeValues": [
                        {
                            "value": data["harvestid"]  
                        }
                    ]
            } 
        ]
        

    def delete_jira_record(self, record_id):
        print("delete:",record_id)

        headers = {
            'Authorization': f'Basic {self.encoded_credentials}',
            'Content-Type': 'application/json'
        } 
        delete_url = f'{self.base_url}/object/{record_id}'

        # Perform Delete
        delete_response = requests.delete(delete_url, headers=headers)
        if delete_response.status_code == 204:
            print("delete response:", delete_response.json() )
            return True
        elif delete_response.status_code == 200:
            print("delete response:", delete_response.json() )
            return True
        else:
            print("Delete failed:", delete_response.status_code, delete_response.text)
            return False
        

    def getVirusObjectID(self, speciesname):
        return self.virus_map[speciesname.lower().strip()]
    
    
    def getTargetObjectID(self, target_id):
        return  self.target_id_map[target_id]
    

    def setTargetIDMap(self, response_list): 
        for item in response_list:  
            attributes = item["attributes"]
            for attr in attributes:
                if attr["objectTypeAttributeId"] == "96":  #Target ID attribute
                    key = attr["objectAttributeValues"][0]["value"].strip() 
                    if key in self.target_id_map:
                        print("Something wrong!! Duplicated Target ID in target_id_map ",key)
                    else:    
                        self.target_id_map[key] = item["id"] 
    

    def getPurProteinObjectID(self, purbatchcproprcid): 
        try:
            if (len(self.all_pur_protein_id_map)!=0) and (purbatchcproprcid in self.all_pur_protein_id_map):
                return self.all_pur_protein_id_map[purbatchcproprcid]
            elif (len(self.pur_protein_id_map)!=0) and (purbatchcproprcid in self.pur_protein_id_map):
                return self.pur_protein_id_map[purbatchcproprcid]
            else:
                return None
        except:
            return None
        '''
        if (len(self.pur_protein_id_map)==0) or (purbatchcproprcid not in self.pur_protein_id_map):
            return None
        try:
            return self.pur_protein_id_map[purbatchcproprcid]
        except:
            return None
        '''
     

    def setPurProteinIDMap(self, response_list, attribute_id):
        try:
            for item in response_list:  
                attributes = item["attributes"] 
                for attr in attributes:
                    if attr["objectTypeAttributeId"] == attribute_id:  #Pur Protein Purbatchcproprcid attribute 326,374
                        key = attr["objectAttributeValues"][0]["value"].strip() 
                        if key in self.pur_protein_id_map:
                            print("Something wrong!! Duplicated Pur protein Purbatchcproprcid in pur_protein_id_map",key)
                        else:    
                            self.pur_protein_id_map[key] = item["id"] 
        except:
            print("Erroe:setPurProteinIDMap response_list:",response_list)
    
    
    def setAllPurProteinIDMap(self, response_list, attribute_id):
        try:
            for item in response_list:  
                attributes = item["attributes"] 
                for attr in attributes:
                    if attr["objectTypeAttributeId"] == attribute_id:  #Pur Protein Purbatchcproprcid attribute 326,374
                        key = attr["objectAttributeValues"][0]["value"].strip() 
                        if key in self.all_pur_protein_id_map:
                            print("Something wrong!! Duplicated Pur protein Purbatchcproprcid in all_pur_protein_id_map",key)
                        else:    
                            self.all_pur_protein_id_map[key] = item["id"] 
        except:
            print("Error:setAllPurProteinIDMap response_list:",response_list)
            
    def remove_purifiedproteins_from_all_purifiedproteins(self, purifiedproteins_json, all_purifiedproteins_json):
        out=[]
        for apitem in all_purifiedproteins_json:
            found = False
            for pitem in purifiedproteins_json: 
                if apitem["purbatchcproprcid"] == pitem["purbatchcproprcid"]:
                    found = True
                    break
                    #print("Found:",pitem["cloneidf"],apitem["cloneidf"])
            if not found:
                out.append(apitem)
         
        return out
    

    def upload_attachment(self, object_id, structure_json):
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/temp_files/"  #current path: /srv/app/core/jira
        filename = 'structure.pdb'

        file_generated = False    
        media = structure_json['media']
        for mediatypes in media:       
            if 'commonname' in mediatypes:
                if mediatypes['commonname'] == 'coordinates':
                    #print ('found coordinates')
                    coordinates = base64.b64decode(mediatypes['filedata'])
                    with open(file_path+"structure.pdb", "w") as f:
                        print(coordinates, file=f)
                        file_generated = True

        if file_generated==False:
            print("No structure file generated, skip attachement.")
            return

        headers = {
        'Authorization': f'Basic {self.encoded_credentials}',
        'Accept': 'application/json'
        }
        credentials_url = f'https://api.atlassian.com/jsm/assets/workspace/{self.workspace_id}/v1/attachments/object/{object_id}/credentials'
        credentials_response = requests.get(credentials_url, headers=headers)
        credentials_data = credentials_response.json()

        # Extract needed values
        client_id = credentials_data['clientId']
        media_base_url = credentials_data['mediaBaseUrl']
        media_jwt_token = credentials_data['mediaJwtToken']

        print(client_id,media_base_url,media_jwt_token)

        # Step 2: Upload the file to the media server
        upload_url = f"{media_base_url}/file/binary?name={quote(filename.split('/')[-1])}"
        upload_headers = {
            'X-Client-Id': client_id,
            'Authorization': f'Bearer {media_jwt_token}',
            'Content-Type': 'multipart/form-data'
        }
            
        file_content = {'file': open(file_path+filename, 'rb')}
        upload_response = requests.post(upload_url, headers=upload_headers, files=file_content)
        upload_data = upload_response.json()

        # Check if the upload was successful before proceeding
        if upload_response.status_code == 200 or upload_response.status_code == 201:
            upload_data = upload_response.json()
            try:
                media_id = upload_data['data']['id']
                media_size = upload_data['data']['size']
            except KeyError as e:
                print(f"Failed to extract necessary information from upload response: {e}")
                print("Response was:", upload_data)
                exit(1)
        else:
            print("Failed to upload file:", upload_response.status_code, upload_response.text)
            exit(1)

        # Step 3: Attach the file to the asset
        attach_url = f'https://api.atlassian.com/jsm/assets/workspace/{self.workspace_id}/v1/attachments/object/{object_id}'
        attach_headers = {
            'Authorization': f'Basic {self.encoded_credentials}',
            'Content-Type': 'application/json'
        }
        attach_data = {
            "attachments": [
                {
                    "contentType": "text/plain",  # Correct MIME type for a text file
                    "filename": filename.split('/')[-1],
                    "mediaId": media_id,
                    "size": media_size,
                    "comment": "Attaching a text file"
                }
            ]
        }
        attach_response = requests.post(attach_url, headers=attach_headers, json=attach_data)

        # Output the result
        if attach_response.status_code in [200, 201]:
            print("Attachment successful:") #, attach_response.json()
        else:
            print("Failed to attach file:", attach_response.status_code, attach_response.text)



