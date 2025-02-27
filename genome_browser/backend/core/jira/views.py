from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
import requests
import urllib
import json
from django.conf import settings
import base64 
import re
from .CSBID import CSBIDTask



def push_to_jira(request):
    CSBID_task = CSBIDTask()
    update_response = CSBID_task.run()
    return JsonResponse(update_response, safe=False)







