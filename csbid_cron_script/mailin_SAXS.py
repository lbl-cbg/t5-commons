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

    ROTKEY = os.environ.get("MAILIN_SAXS_API_ROTKEY")
    shift_id_map = {}


    def __init__(self):
        logger.debug("Start Mailin SAXS task...")
        self.workspace_id = super().get_workspace_id() 
        self.base_url =  super().get_base_url(self.workspace_id) 



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
                            'X-ROTKEY': self.ROTKEY}

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



