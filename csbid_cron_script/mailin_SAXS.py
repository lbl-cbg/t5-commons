import requests 
import json
import base64 
import re
import os
from datetime import datetime
from jira_import_util import JIRAImportUtil, JIRAObjectID
import logging
logger = logging.getLogger("Logger") 


class MailinSAXSTask(JIRAImportUtil):    

    API_TOKEN = os.environ.get("SIMPLESCATTERING_API_TOKEN")
    ROTKEY = os.environ.get("SIMPLESCATTERING_API_ROTKEY") 
    username = 'Yeongshnn-Ong-T5'
    credentials = f'{username}:{API_TOKEN}'
    credentials = credentials.encode("ascii")
    base64_bytes = base64.b64encode(credentials)
    encoded_credentials = base64_bytes.decode("ascii")
    #encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8') 

    JIRA_API_TOKEN_YONG = os.environ.get("JIRA_API_TOKEN_YONG")
    username = "yong@lbl.gov"
    jira_credentials = f'{username}:{JIRA_API_TOKEN_YONG}'
    jira_encoded_credentials = base64.b64encode(jira_credentials.encode('utf-8')).decode('utf-8')  
    jira_servicedeskapi_url = 'https://taskforce5.atlassian.net/rest/servicedeskapi/assets/workspace'
    shift_id_map = {}

    def __init__(self):
        logger.debug("Start Mailin SAXS task...")
        self.workspace_id = self.get_workspace_id()
        self.base_url = self.get_base_url(self.workspace_id)


    def get_workspace_id(self):
        response = requests.get(self.jira_servicedeskapi_url, 
                                headers={'Content-Type': 'application/json', 
                                        'authorization': 'Basic ' + self.jira_encoded_credentials})
        workspaces_response_json = response.json() 
        print("JIRA workspaces:", workspaces_response_json)
        return workspaces_response_json["values"][0]["workspaceId"]
 

    def get_base_url(self, workspace_id):
        return f'https://api.atlassian.com/jsm/assets/workspace/{workspace_id}/v1'


    def run(self):
        json_data = self.get_data()
        
        shifts_json = json_data["shifts_json"]      
        attributes_data_list = []
        for data_item in shifts_json:
            attributes_data = self.get_shift_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.SHIFTS.value,
                                    shifts_json,
                                    attributes_data_list,
                                    "Shifts",
                                    "Id", 
                                    "id") 
        self.setShiftIDMap(create_record_response, "503")

        t5slots_samples_json = json_data["t5slots_samples_json"]      
        attributes_data_list = []
        for data_item in t5slots_samples_json:
            attributes_data = self.get_slot_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.SLOTS_W_SAMPLES.value,
                                    t5slots_samples_json,
                                    attributes_data_list,
                                    "Slots with Samples",
                                    "Sample Id", 
                                    "id") 
        
        return json_data
    

    def get_data(self):
        print('Get mailin SAXS data...') 

        mailin_saxs_url = "https://sibyls.als.lbl.gov/htsaxs/api/v1"
        mailin_saxs_header = {'Content-Type': 'application/json',  
                            'X-ROTKEY': '3f59d82ec81fc9ea1447f077377ba3aa1235b9f9bdb89c86816a71f104a69b4e'}

        response = requests.get(mailin_saxs_url + "/shifts", 
                                headers=mailin_saxs_header)
        shifts_json = response.json()
        
        #response = requests.get(mailin_saxs_url + "/t5slots", 
        #                        headers=mailin_saxs_header)
        #slots_json = response.json()

        response = requests.get(mailin_saxs_url + "/t5slots_samples", 
                                headers=mailin_saxs_header)
        t5slots_samples_json = response.json()
         
        return {"shifts_json":shifts_json,
                #"slots_json":slots_json,
                "t5slots_samples_json":t5slots_samples_json}


    def get_shift_attributes_data(self, data):
        return [
            {
                    "objectTypeAttributeId": "502", 
                    "objectAttributeValues": [
                        {
                            "value": data["created_at"] #datetime.strptime(data["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%dT%H:%M:%S.%fZ") 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "503", 
                    "objectAttributeValues": [
                        {
                            "value": data["id"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "504", 
                    "objectAttributeValues": [
                        {
                            "value": data["shift_type"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "505", 
                    "objectAttributeValues": [
                        {
                            "value": data["shifts"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "506", 
                    "objectAttributeValues": [
                        {
                            "value": data["start_date"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "507", 
                    "objectAttributeValues": [
                        {
                            "value": data["status"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "508", 
                    "objectAttributeValues": [
                        {
                            "value": data["updated_at"] #datetime.strptime(data["updated_at"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%dT%H:%M:%S.%fZ")   
                        }
                    ]
            } 
        ] 



    def get_slot_attributes_data(self, data):  
        return [
            {
                    "objectTypeAttributeId": "509", 
                    "objectAttributeValues": [
                        {
                            "value": data["created_at"]  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "510", 
                    "objectAttributeValues": [
                        {
                            "value": data["date_available"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "511", 
                    "objectAttributeValues": [
                        {
                            "value": data["date_awaiting"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "512", 
                    "objectAttributeValues": [
                        {
                            "value": data["date_collected"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "513", 
                    "objectAttributeValues": [
                        {
                            "value": data["date_received"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "514", #Sample Id
                    "objectAttributeValues": [
                        {
                            "value": data["id"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "515", 
                    "objectAttributeValues": [
                        {
                            "value": data["note"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "516", 
                    "objectAttributeValues": [
                        {
                            "value": data["shift_id"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "536", 
                    "objectAttributeValues": [
                        {
                            "value": self.getShiftObjectID(data["shift_id"])  
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "517", 
                    "objectAttributeValues": [
                        {
                            "value": data["shipping_company"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "518", 
                    "objectAttributeValues": [
                        {
                            "value": data["status"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "519", 
                    "objectAttributeValues": [
                        {
                            "value": data["tag"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "520", 
                    "objectAttributeValues": [
                        {
                            "value": data["taskforce5"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "521", 
                    "objectAttributeValues": [
                        {
                            "value": data["tracking_number"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "522", 
                    "objectAttributeValues": [
                        {
                            "value": data["updated_at"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "523", #Shift Id
                    "objectAttributeValues": [
                        {
                            "value": data["user_id"]    
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "524", 
                    "objectAttributeValues": [
                        {
                            "value": json.dumps(data["sec_samples"])    
                        }
                    ]
            }
        ] 
    

    def getShiftObjectID(self, id):
        return self.shift_id_map[str(id)]


    def setShiftIDMap(self, response_list, attribute_id):
        try:
            for item in response_list:  
                attributes = item["attributes"] 
                for attr in attributes:
                    if attr["objectTypeAttributeId"] == attribute_id:  #id 503
                        key = attr["objectAttributeValues"][0]["value"].strip() 
                        if key in self.shift_id_map:
                            print("Something wrong!! Duplicated Pur protein Purbatchcproprcid in shift_id_map",key)
                        else:    
                            self.shift_id_map[key] = item["id"] 
        except:
            print("Erroe:setShiftIDMap response_list:",response_list)






    '''
    print("simplescattering =======>")
    simplescattering_url = 'https://simplescattering.com' # 'https://simple-saxs-staging.herokuapp.com'
    username = 'Yeongshnn-Ong-T5'
    s = f'{username}:{self.API_TOKEN}'
    s = s.encode("ascii")
    base64_bytes = base64.b64encode(s)
    ttt_token = base64_bytes.decode("ascii")
    
    response = requests.post(simplescattering_url+"/api-keys", 
                            headers={'Content-Type': 'application/json', 
                                     'authorization': 'Basic '+ttt_token,
                                     'ROTKEY': self.ROTKEY})
    print(response.text) 

    d_tokens = response.text.split() 
    print(d_tokens)
    data_response = requests.get(simplescattering_url+"/api/v1/t5_datasets", 
                            headers={'Content-Type': 'application/json', 
                                     'Authorization': 'Bearer '+d_tokens[0],
                                     'ROTKEY': self.ROTKEY})
    print(data_response.text)

    print("simplescattering <=======")
    '''