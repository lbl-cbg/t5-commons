import json
import os
import sys
import requests


class JAMOConnector:

    DONE_STATE = '41'

    def __init__(self, jamo_host=None, jamo_token=None):
        # Connect to Jira
        self.host = jamo_host or os.environ.get('JAMO_HOST', 'https://data-dev.taskforce5.lbl.gov')
        jamo_token = jamo_token or os.environ['JAMO_TOKEN']
        self.auth = {'Authorization': f"Application {os.environ['JAMO_TOKEN']}"}

    def __get(self, url, params=None):
        # Make the request to get the asset details
        headers = {
            "Accept": "application/json"
        } | self.auth
        response = requests.get(url, headers=headers, params=params)
        return response

    def __put(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        } | self.auth
        response = requests.put(url, headers=headers, data=json.dumps(data))
        return response

    def __post(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        } | self.auth
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response

    def get(self, url, params=None):
        url = f'{self.host}/api/{url}'
        return self.__get(url, params=params)

    def put(self, url, data):
        url = f'{self.host}/api/{url}'
        return self.__put(url, data)

    def post(self, url, data):
        url = f'{self.host}/api/{url}'
        return self.__post(url, data)

    def create_analysis(self, directory, template_name, template_data, source='nersc'):
        payload = {
                'template_name': template_name,
                'template_data': template_data,
                'source': source,
                'location': os.path.abspath(directory)
        }
        return self.post("analysis/analysisimport", payload)

    def search(self, query):
        return self.get("metadata/query", params=query)
