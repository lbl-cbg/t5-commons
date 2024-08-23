import requests 
import json
from django.conf import settings
import base64 
import re
import os
from datetime import datetime


class MailinSAXSTask:    

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


    def __init__(self):
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

        #shifts_json = json_data["shifts_json"]     
        #create_record_response = self.create_new_shifts_jira_record(shifts_json)

        t5slots_samples_json = json_data["t5slots_samples_json"]     
        create_record_response = self.create_new_t5slots_samples_jira_record(t5slots_samples_json)

        return json_data
    

    def get_data(self):
        print('Get data...') 

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


    def create_new_shifts_jira_record(self, data):
        for data_item in data:
            found = self.check_jira_record_exit(data_item, "Id", "id")    
            attributes_data = self.get_shift_attributes_data(data_item)
            if found:
                print("found, update existing...")
                self.update_record(data_item, 29, found[0]["id"], attributes_data) #Shift object id:29
            else:
                print("not found, create new...")
                self.push_new_record(data_item, 29, attributes_data) #Shift object id:29
        return None
    

    def create_new_t5slots_samples_jira_record(self, data):
        for data_item in data:
            found = self.check_jira_record_exit(data_item, "Id", "id")    
            attributes_data = self.get_slot_attributes_data(data_item)
            if found:
                print("found, update existing...")
                self.update_record(data_item, 30, found[0]["id"], attributes_data ) #Slot object id:30
            else:
                print("not found, create new...")
                self.push_new_record(data_item, 30, attributes_data) #Slot object id:30
        return None


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
                    "objectTypeAttributeId": "514", 
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
                    "objectTypeAttributeId": "523", 
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


    def update_record(self, data, object_id, record_id, attributes_data):
        print("Update jira record...")  
     
        create_data = {
            "objectTypeId": object_id,  # Create a object
            "attributes": attributes_data,
            "hasAvatar": False  # Optional avatar
        }

        base_url = f'{self.base_url}/object/{record_id}'
        headers = {
                    'Authorization': f'Basic {self.jira_encoded_credentials}',
                    'Content-Type': 'application/json'
        }

        # Perform the POST request to create a new asset record
        create_response = requests.put(base_url, headers=headers, data=json.dumps(create_data))

        # Check the response and handle accordingly
        if create_response.status_code in [200, 201]:
            create_results = create_response.json()
            print("Asset update successful:") #, create_results
            return create_results
        else:
            print("Failed to update asset:", create_response.status_code, create_response.text)
            return None
    

    def push_new_record(self, data, object_id, attributes_data):
        print("Create new jira record...")  
     
        create_data = {
            "objectTypeId": object_id,  # Create a object
            "attributes": attributes_data,
            "hasAvatar": False  # Optional avatar
        }

        base_url = f'{self.base_url}/object/create'
        headers = {
                    'Authorization': f'Basic {self.jira_encoded_credentials}',
                    'Content-Type': 'application/json'
        }

        # Perform the POST request to create a new asset record
        create_response = requests.post(base_url, headers=headers, data=json.dumps(create_data))
        
        print("create_response:",create_response)

        # Check the response and handle accordingly
        if create_response.status_code in [200, 201]:
            create_results = create_response.json()
            print("Asset creation successful:") #, create_results
            return create_results
        else:
            print("Failed to create asset:", create_response.status_code, create_response.text)
            return None
    


    def check_jira_record_exit(self, data_item, jira_id_field_name, id_field_name):
        print("id to search:",data_item[id_field_name])

        headers = {
            'Authorization': f'Basic {self.jira_encoded_credentials}',
            'Content-Type': 'application/json'
        }
        # IQL search query
        iql_query = f'{jira_id_field_name} == "{data_item[id_field_name]}" '
        search_url = f'{self.base_url}/object/aql'
        search_data = {
            "qlQuery": iql_query
        }

        # Perform the IQL search\
        search_response = requests.post(search_url, headers=headers, data=json.dumps(search_data))
        if search_response.status_code == 200:
            search_results = search_response.json()
            if 'values' in search_results and len(search_results['values']) > 0: 
                print("found")#print("Search results:", search_results['values'])
                return search_results["values"]
            else:
                print("No objects found with the specified query.")
                return False
        else:
            print("Search failed:", search_response.status_code, search_response.text)
            return False
    









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