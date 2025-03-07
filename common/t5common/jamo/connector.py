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

    def __get(self, url):
        # Make the request to get the asset details
        headers = {
            "Accept": "application/json"
        } | self.auth
        response = requests.get(url, headers=headers, auth=self.auth)
        if response.status_code not in [200, 201, 204]:
            print(f"GET FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)
        return response.json()

    def __put(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        } | self.auth
        response = requests.put(url, headers=headers, auth=self.auth, data=json.dumps(data))
        if response.status_code not in [200, 201, 204]:
            print(f"PUT FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)

        return None if response.status_code == 204 else response.json()

    def __post(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        } | self.auth
        response = requests.post(url, headers=headers, auth=self.auth, data=json.dumps(data))
        if response.status_code not in [200, 201, 204]:
            print(f"POST FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)

        return None if response.status_code == 204 else response.json()

    def get(self, url):
        url = f'{self.host}/api/{url}'
        return self.__get(url)

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

    def query(self, **kwargs):
        return self.get("file/query", **kwargs)
