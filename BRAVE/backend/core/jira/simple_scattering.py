import requests 
import json
from django.conf import settings
import base64 
import re
import os


class SimpleScatteringTask:    

    API_TOKEN = os.environ.get("SIMPLESCATTERING_API_TOKEN")
    ROTKEY = os.environ.get("SIMPLESCATTERING_API_ROTKEY") 

    #def __init__(self):


    def run(self):
        return None
    

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