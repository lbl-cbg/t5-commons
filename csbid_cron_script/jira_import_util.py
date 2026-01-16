
import requests 
import json
import base64 
import os
from abc import ABC, abstractmethod
from enum import Enum

class JIRAObjectID(Enum):
    TARGET = 9 
    CONSTRUCT = 10
    All_PURIFIED_PROTEINS = 24
    PROTEIN_INVENTORY = 23
    PURIFIED_PROTEINS = 22
    ALL_CRYSTAL_SUMMARY = 25
    CRYSTAL = 26
    STRUCTURE = 31
    SIMPLE_SCATTERING = 27
    SLOTS_W_SAMPLES = 30
    SHIFTS = 29


class JIRAImportUtil(ABC):

    JIRA_API_TOKEN_YONG = os.environ.get("JIRA_API_TOKEN_YONG")
    username = os.environ.get("JIRA_USERNAME")
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

    
    def create_new_jira_record(self, jira_object_id, data_list, attributes_data_list, jira_record_search_object_type, jira_record_search_id, data_search_id):
        """
        Create or update a jira record .
 
        Parameters:
        data_list (list):  list of data to push to jira.
        attributes_data_list: a list of attributes data/payload
        jira_record_search_id (str): jira id to search
        data_search_id (str): key for data_item search field 

        Returns:
        return_type: Description of the return value.
        """ 
        req_result_list = []
        for index, data_item in enumerate(data_list):
            try:
                found = self.check_jira_record_exit(data_item, jira_record_search_object_type, jira_record_search_id, data_search_id)  
                attributes_data = attributes_data_list[index] #self.get_attributes_data(data_item) 
                if found:
                    print("found, update existing...")
                    req_result = self.update_record(jira_object_id, found[0]["id"], attributes_data)  
                else:
                    print("not found, create new...")
                    req_result = self.push_new_record(jira_object_id, attributes_data)  
                req_result_list.append(req_result)
            except:
                req_result_list.append(None)
        return req_result_list
    

    #@abstractmethod
    #def get_attributes_data(self):
    #    pass
    
    
    def check_jira_record_exit(self, data_item, jira_record_search_object_type, jira_id_field_name, id_field_name):
        search_str = ""
        if isinstance(id_field_name, list):
            for field in id_field_name:
                search_str = search_str + data_item[field].strip() + "_"
            search_str = search_str[:-1]
        else:
            search_str = str(data_item[id_field_name]).strip()
        #print("id to search:",search_str)

        headers = {
            'Authorization': f'Basic {self.jira_encoded_credentials}',
            'Content-Type': 'application/json'
        }
        # IQL search query
        iql_query = f'objectType = "{jira_record_search_object_type}" AND "{jira_id_field_name}" = "{search_str}" '    #f'objectType = "{jira_record_search_object_type}" AND "{jira_id_field_name}" == "{data_item[id_field_name]}" '
        search_url = f'{self.base_url}/object/aql'
        search_data = {
            "qlQuery": iql_query
        }
        print("search query::", search_data)
        # Perform the IQL search\
        search_response = requests.post(search_url, headers=headers, data=json.dumps(search_data))  #json.dumps(search_data)
        if search_response.status_code == 200:
            search_results = search_response.json()
            if 'values' in search_results and len(search_results['values']) > 0: 
                #print("found")#print("Search results:", search_results['values'])
                return search_results["values"]
            else:
                #print("No objects found with the specified query.")
                return False
        else:
            print("Search failed:", search_response.status_code, search_response.text)
            return False
        
    
    def update_record(self, object_id, record_id, attributes_data):
        """
        Update a jira record .
 
        Parameters:
        object_id:  jira object id
        record_id: the record to be updated
        attributes_data: push payload 

        Returns:
        result: push result
        """
        #print("Update jira record...")  
     
        create_data = {
            "objectTypeId": object_id,   
            "attributes": attributes_data,
            "hasAvatar": False   
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
    

    def push_new_record(self, object_id, attributes_data):
        """
        Push new jira record .
 
        Parameters:
        object_id:  jira object id
        attributes_data: push payload 

        Returns:
        result: push result
        """
        #print("Create new jira record...")  
     
        create_data = {
            "objectTypeId": object_id,   
            "attributes": attributes_data,
            "hasAvatar": False   
        }
         
        base_url = f'{self.base_url}/object/create'
        headers = {
                    'Authorization': f'Basic {self.jira_encoded_credentials}',
                    'Content-Type': 'application/json'
        } 
        # Perform the POST request to create a new asset record
        create_response = requests.post(base_url, headers=headers, data=json.dumps(create_data))
        #print("create_response:",create_response)

        # Check the response and handle accordingly
        if create_response.status_code in [200, 201]:
            create_results = create_response.json()
            #return create_results
            print("Asset creation successful:") #, create_results
            get_obj_url = f'{self.base_url}/object/{create_results["id"]}'
            get_obj_response = requests.get(get_obj_url, headers=headers)
            return get_obj_response.json()
        else:
            print("Failed to create asset:", create_response.status_code, create_response.text)
            return None