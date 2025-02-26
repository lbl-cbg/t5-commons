import argparse
import json
import os
import sys
import requests
from requests.auth import HTTPBasicAuth
from sdm_curl import Curl

DONE_STATE = '41'
TARGET_ID_ID = '97'  # the asset attribute id to use for finding the protein target_id
PARENT_NAME_ID = '491'


class JiraConnector:

    def __init__(self):
        # Connect to Jira
        self.jira_server = os.environ.get('JIRA_HOST', 'https://taskforce5.atlassian.net')
        self.auth = HTTPBasicAuth(os.environ.get('JIRA_USER', 'ajtritt@lbl.gov'), os.environ['JIRA_TOKEN'])
        # Set up headers for the request
        self.workspace_id = self.get("servicedeskapi/assets/workspace")["values"][0]["workspaceId"]
        self.workspace_url = f'https://api.atlassian.com/jsm/assets/workspace/{self.workspace_id}/v1'

    def __get(self, url):
        # Make the request to get the asset details
        headers = {
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, auth=self.auth)
        if response.status_code not in [200, 201, 204]:
            print(f"GET FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)
        return response.json()

    def __put(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.put(url, headers=headers, auth=self.auth, data=json.dumps(data))
        if response.status_code not in [200, 201, 204]:
            print(f"PUT FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)

        return None if response.status_code == 204 else response.json()

    def __post(self, url, data):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.post(url, headers=headers, auth=self.auth, data=json.dumps(data))
        if response.status_code not in [200, 201, 204]:
            print(f"POST FAIL {url}: {response.status_code} - {response.text}", file=sys.stderr)
            exit(1)

        return None if response.status_code == 204 else response.json()

    def get(self, url):
        url = f'{self.jira_server}/rest/{url}'
        return self.__get(url)

    def put(self, url, data):
        url = f'{self.jira_server}/rest/{url}'
        return self.__put(url, data)

    def post(self, url, data):
        url = f'{self.jira_server}/rest/{url}'
        return self.__post(url, data)

    def create_asset(self, data):
        url = f'{self.workspace_url}/object/create'
        headers = {
                    'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, auth=self.auth, data=json.dumps(data))
        if response.status_code not in [200, 201]:
            print("Failed to create asset:", response.status_code, response.text, file=sys.stderr)
            exit(1)
        return response.json()

    def get_asset(self, object_id):
        url = f"https://api.atlassian.com/jsm/assets/workspace/{self.workspace_id}/v1/object/{object_id}"
        return self.__get(url)


def _add_field(asset_data, attr_id, value_key, payload, required=True):
    value = asset_data.get(value_key)
    if value is not None:
        payload.append({
                "objectTypeAttributeId": attr_id,
                "objectAttributeValues": [{'value': value}]
        })
    elif required:
        raise ValueError(f"Required key {value_key} not provided")


def make_bilbo_asset(**data):
    asset_data = list()
    _add_field(data, 613, 'name', asset_data) # JAMO URL
    _add_field(data, 616, 'jamo_url', asset_data) # JAMO URL
    _add_field(data, 684, 'bilbomd_url', asset_data, required=False) # BilboMD URL # TODO: sclassen - set to required=True when this gets added
    _add_field(data, 681, 'ss_asset', asset_data) # Link to SimpleScattering asset
    _add_field(data, 682, 'target_asset', asset_data) # Link to Target asset
    _add_field(data, 683, 'ss_filename', asset_data) # Filename in SimpleScattering record
    create_data = {
        "objectTypeId": 34,  # Create a "BilboMD Result" object
        "attributes": asset_data,
        "hasAvatar": False  # Optional avatar
    }
    return create_data


parser = argparse.ArgumentParser()
parser.add_argument("bilbomd_dir", help="the output directory from a BilboMD run")
parser.add_argument("jira_issue", help="the Jira issue for this BilboMD run")

args = parser.parse_args()

# Load the JIRA issue and get metadata needed to populate JAT record
jc = JiraConnector()
issue = jc.get(f"api/3/issue/{args.jira_issue}")
target_asset = jc.get_asset(issue['fields']['customfield_10113'][0]['objectId'])

target_id = None
virus_id = None
for attr in target_asset['attributes']:
    if attr['objectTypeAttribute']['id'] == TARGET_ID_ID:
        target_id = attr['objectAttributeValues'][0]['value']
    elif attr['objectTypeAttribute']['id'] == PARENT_NAME_ID:
        virus_id = attr['objectAttributeValues'][0]['referencedObject']['name']

if virus_id is None:
    print(f"Unable to get virus name from issue {args.jira_issue}", file=sys.stderr)
    exit(1)
if target_id is None:
    print(f"Unable to get protein target from issue {args.jira_issue}", file=sys.stderr)
    exit(1)


# Submit to JAT if not already submitted
metadata_json = os.path.join(args.bilbomd_dir, 'metadata.json')
jat_key = None
jat_key_file = os.path.join(args.bilbomd_dir, 'jat_key')
host = os.environ['JAMO_HOST']
if not os.path.exists(metadata_json):
    # Build payload for posting analysis to JAMO
    outputs = []
    inputs = []
    metadata = {'jira_issue': args.jira_issue,
                'virus_id': virus_id,
                'protein_id': target_id}
    for file in os.listdir(args.bilbomd_dir):
        path = os.path.realpath(os.path.join(args.bilbomd_dir, file))
        file_metadata = dict()
        if file.startswith("ensemble"):
            file_metadata['ensemble_size'] = int(os.path.splitext(file)[0].split("_")[2])
            if file.endswith("pdb"):
                file_metadata['file_format'] = 'pdb'
                file_metadata['compression'] = 'none'
                label = 'protein_model'
            else:
                file_metadata['file_format'] = 'txt'
                file_metadata['compression'] = 'none'
                label = 'ensembles_info'
        elif file == 'const.inp':
            file_metadata['file_format'] = 'txt'
            file_metadata['compression'] = 'none'
            label = 'const_inp'
        elif file.startswith('README'):
            file_metadata['file_format'] = file.split('.')[-1]
            file_metadata['compression'] = 'none'
            label = 'readme'
        elif file.startswith('multi_state_model'):
            file_metadata['ensemble_size'] = int(file.split("_")[3])
            file_metadata['file_format'] = 'saxs_dat'
            file_metadata['compression'] = 'none'
            label = 'theoretical_saxs_curve'
        else:
            continue
        outputs.append({'file': path, 'label': label, 'metadata': file_metadata})

    # template data
    td = {'outputs': outputs, 'metadata': metadata, 'inputs': inputs}

    curl = Curl(host, appToken=os.environ['BILBOMD_JAMO_TOKEN'])  # BilboMD application token
    resp = curl.post('api/analysis/analysisimport',
                     template_name='bilbomd_classic_results',
                     template_data=td,
                     source="nersc",
                     location=os.path.abspath(args.bilbomd_dir))

    jat_key = resp['jat_key']

    if len(resp['warnings']) > 0:
        sys.stdout.write('\n'.join(resp['warnings']) + '\n')
    print(f"Successfully imported results from {args.bilbomd_dir} as {jat_key}")

    with open(metadata_json, 'w') as f:
        json.dump(td, f, indent=4)

    with open(jat_key_file, "w") as f:
        print(jat_key, file=f)
else:
    if not os.path.exists(jat_key_file):
        print(f"Found metadata.json file in {args.bilbo_dir} but no JAT key file (jat_key)", file=sys.stderr)
        exit(1)
    with open(jat_key_file, "r") as f:
        jat_key = f.read().strip()
    print("Data already submitted to JAT")

jamo_url = os.path.join(host, 'analysis/analysis', jat_key)
print(f"You can view analysis at {jamo_url}")


# Create Bilbo Asset and update Issue
print("Creating asset in Jira for BilboMD")
new_bilbo_asset = make_bilbo_asset(name=f"{issue['key']} result",
                                   ss_asset=issue['fields']['customfield_10108'][0]['objectId'],
                                   target_asset=issue['fields']['customfield_10113'][0]['objectId'],
                                   ss_filename=issue['fields']['customfield_10115'],
                                   bilbomd_url=None, #TODO: sclassen - add code to set this
                                   jamo_url=jamo_url)
bilbo_asset = jc.create_asset(new_bilbo_asset)

update_data = {"fields": {"customfield_10114": [{'objectId': bilbo_asset['id'],
                                                 'workspaceId': bilbo_asset['workspaceId'],
                                                 'id': bilbo_asset['globalId']}]}}
print(f"Updating issue {args.jira_issue} with asset {bilbo_asset['id']}")
jc.put(f"api/3/issue/{args.jira_issue}", update_data)

print(f"Closing issue {args.jira_issue}")
transition_data = {"transition": {"id": DONE_STATE}}
jc.post(f"api/3/issue/{args.jira_issue}/transitions", transition_data)
