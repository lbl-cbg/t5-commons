import requests 
import json
from django.conf import settings
import base64 
import re
import os


class SimpleScatteringTask:    


    #def __init__(self):


    def run(self):
        return None
    

    '''
    print("simplescattering =======>")
    simplescattering_url = 'https://simplescattering.com' # 'https://simple-saxs-staging.herokuapp.com'
    
    s = 'Yeongshnn-Ong-T5:eaf53e7fb09391660a39a8af739d05ec'
    s = s.encode("ascii")
    base64_bytes = base64.b64encode(s)
    ttt_token = base64_bytes.decode("ascii")
    
    response = requests.post(simplescattering_url+"/api-keys", 
                            headers={'Content-Type': 'application/json', 
                                     'authorization': 'Basic '+ttt_token,
                                     'ROTKEY':'996618fafa1e241a29979e6f3ad5643237621d5cf7327bbacf4752708773b492'})
    print(response.text) 

    d_tokens = response.text.split() 
    print(d_tokens)
    data_response = requests.get(simplescattering_url+"/api/v1/t5_datasets", 
                            headers={'Content-Type': 'application/json', 
                                     'Authorization': 'Bearer '+d_tokens[0],
                                     'ROTKEY':'996618fafa1e241a29979e6f3ad5643237621d5cf7327bbacf4752708773b492'})
    print(data_response.text)


    #curl -X POST https://simplescattering.com/api-keys -H "Content-Type: application/json" -H "ROTKEY: 996618fafa1e241a29979e6f3ad5643237621d5cf7327bbacf4752708773b492" -u Yeongshnn-Ong-T5:eaf53e7fb09391660a39a8af739d05ec

    
    req = urllib.request.Request(simplescattering_url+"/api-keys" , 
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                                          'Content-Type':'application/json', 
                                          'authorization':'Basic Yeongshnn-Ong-T5:eaf53e7fb09391660a39a8af739d05ec',
                                          'ROTKEY':'996618fafa1e241a29979e6f3ad56432' + '37621d5cf7327bbacf4752708773b492'},
                                )
     
    response = urllib.request.urlopen(req) 
    content = response.read().decode(response.info().get_param('charset') or 'utf-8')
    print(content) 
    
    m = re.search(r'(?<=csrf-token).+(?=\>)', content)
    #print(m.group(0))
    m2 = re.search(r'(?<=content=").+(?=["])', m.group(0))
    #print(m2.group(0))
    simplescattering_token = m2.group(0)
    print(simplescattering_token)



    req = urllib.request.Request(simplescattering_url+'/api/v1/t5_datasets', 
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                                          'Content-Type':'application/json', 
                                          'ROTKEY':'996618fafa1e241a29979e6f3ad56432' + '37621d5cf7327bbacf4752708773b492',
                                          'authorization':'Bearer ' + simplescattering_token
                                          },
                                )
     
    response = urllib.request.urlopen(req)
    print(response)
    content = response.read().decode(response.info().get_param('charset') or 'utf-8')
    print(content) 
    
    print("simplescattering <=======")
    '''