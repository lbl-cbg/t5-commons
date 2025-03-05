import requests 
import json
import base64 
import re
import os
from datetime import datetime
from jira_import_util import JIRAImportUtil, JIRAObjectID
import logging
logger = logging.getLogger("Logger") 


class SimpleScatteringTask(JIRAImportUtil):    

    API_TOKEN = os.environ.get("SIMPLESCATTERING_API_TOKEN")
    ROTKEY = os.environ.get("SIMPLESCATTERING_API_ROTKEY") 
    username = os.environ.get("SIMPLESCATTERING_API_USERNAME")
    credentials = f'{username}:{API_TOKEN}'
    credentials = credentials.encode("ascii")
    base64_bytes = base64.b64encode(credentials)
    encoded_credentials = base64_bytes.decode("ascii")

    def __init__(self):
        logger.debug("Start Simple scattering task...")
        self.workspace_id = super().get_workspace_id() 
        self.base_url = super().get_base_url(self.workspace_id) 


    def run(self):
        json_data = self.get_data()

        simplescattering_json = json_data["simplescattering_json"]     
        #create_record_response = self.create_new_simplescattering_jira_record(simplescattering_json)
        attributes_data_list = []
        for data_item in simplescattering_json:
            attributes_data = self.get_attributes_data(data_item) 
            attributes_data_list.append(attributes_data) 
        create_record_response = super().create_new_jira_record(
                                    JIRAObjectID.SIMPLE_SCATTERING.value,
                                    simplescattering_json,
                                    attributes_data_list,
                                    "Simple Scattering",
                                    "Code", 
                                    "code") 

        return json_data
    

    def get_data(self):
        print('Get simple scattering data...')  

        simplescattering_url = 'https://simplescattering.com' #prod  #'https://simple-saxs-staging.herokuapp.com' #staging

        response = requests.post(simplescattering_url+"/api-keys", 
                                headers={'Content-Type': 'application/json', 
                                        'authorization': 'Basic ' + self.encoded_credentials,
                                        'ROTKEY': self.ROTKEY})
        print("simple scattering api token:",response.text) 
        data_tokens = response.text.split() 
        token = data_tokens[0]
    
        response = requests.get(simplescattering_url+"/api/v1/t5_datasets", 
                            headers={'Content-Type': 'application/json', 
                                    'Authorization': 'Bearer '+token,
                                    'ROTKEY': self.ROTKEY})
        simplescattering_json = response.json()
        

        '''
        # Note: for another request, need request token again
        response = requests.post(simplescattering_url+"/api-keys", 
                                headers={'Content-Type': 'application/json', 
                                        'authorization': 'Basic ' + self.encoded_credentials,
                                        'ROTKEY': self.ROTKEY})
        print("token:",response.text) 
        data_tokens = response.text.split() 
        token = data_tokens[0]

        print("Get Simple Scattering Show data...")
        response = requests.get(simplescattering_url+"/api/v1/t5_dataset/"+"XSXEB4SL", 
                            headers={'Content-Type': 'application/json', 
                                     'Authorization': 'Bearer '+token,
                                     'ROTKEY': self.ROTKEY})
        print(response.text)
        simplescattering_show_json = response.json()
        '''

        return {"simplescattering_json":simplescattering_json}

    '''
    def create_new_simplescattering_jira_record(self, data):
        for data_item in data:
            found = self.check_jira_record_exit(data_item, "Code", "code")   
            #print("found jira record?", found) 
            if found:
                print("found, update existing...")
                self.update_record(data_item, found[0]["id"])
            else:
                print("not found, create new...")
                self.push_new_record(data_item)
        return None
    '''

    def get_attributes_data(self, data):
        return [
            {
                    "objectTypeAttributeId": "459", 
                    "objectAttributeValues": [
                        {
                            "value": data["experimental_description"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "460", 
                    "objectAttributeValues": [
                        {
                            "value": data["file_description"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "461", 
                    "objectAttributeValues": [
                        {
                            "value": data["data_collection_technique"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "462", 
                    "objectAttributeValues": [
                        {
                            "value": data["authors"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "463", 
                    "objectAttributeValues": [
                        {
                            "value": data["country"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "464", 
                    "objectAttributeValues": [
                        {
                            "value": data["user_id"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "465", 
                    "objectAttributeValues": [
                        {
                            "value": datetime.strptime(data["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%dT%H:%M:%S.%fZ") #data["created_at"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "466", 
                    "objectAttributeValues": [
                        {
                            "value": datetime.strptime(data["updated_at"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%dT%H:%M:%S.%fZ") #data["updated_at"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "467", 
                    "objectAttributeValues": [
                        {
                            "value": data["title"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "468", 
                    "objectAttributeValues": [
                        {
                            "value": data["code"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "469", 
                    "objectAttributeValues": [
                        {
                            "value": data["abstract"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "470", 
                    "objectAttributeValues": [
                        {
                            "value": data["institute_id"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "471", 
                    "objectAttributeValues": [
                        {
                            "value": data["project_leader_id"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "472", 
                    "objectAttributeValues": [
                        {
                            "value": data["slug"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "473", 
                    "objectAttributeValues": [
                        {
                            "value": data["status"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "474", 
                    "objectAttributeValues": [
                        {
                            "value": data["status_toggled_on"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "475", 
                    "objectAttributeValues": [
                        {
                            "value": data["mailin_slot"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "476", 
                    "objectAttributeValues": [
                        {
                            "value": data["mailin_shift"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "477", 
                    "objectAttributeValues": [
                        {
                            "value": data["mailin_sample"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "478", 
                    "objectAttributeValues": [
                        {
                            "value": data["mailin_code"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "479", 
                    "objectAttributeValues": [
                        {
                            "value": data["wavelength"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "480", 
                    "objectAttributeValues": [
                        {
                            "value": data["sample_to_detector_distance"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "481", 
                    "objectAttributeValues": [
                        {
                            "value": data["journal_doi"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "482", 
                    "objectAttributeValues": [
                        {
                            "value": data["source"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "483", 
                    "objectAttributeValues": [
                        {
                            "value": data["beamline"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "484", 
                    "objectAttributeValues": [
                        {
                            "value": data["taskforce5"] 
                        }
                    ]
            },
            {
                    "objectTypeAttributeId": "485", 
                    "objectAttributeValues": [
                        {
                            "value": json.dumps(data["samples"])
                        }
                    ]
            }
        ] 

    '''
    def update_record(self, data, record_id):
        print("Update jira record...")  
     
        create_data = {
            "objectTypeId": "27",  # Create a "Simple scattering" object
            "attributes": self.get_attributes_data(data),
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
    


    def push_new_record(self, data):
        print("Create new jira record...")  
     
        create_data = {
            "objectTypeId": "27",  # Create a "Simple scattering" object
            "attributes": self.get_attributes_data(data),
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
    