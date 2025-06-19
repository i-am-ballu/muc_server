from django.shortcuts import render

# Create your views here.
import requests
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json

# ✅ Common response function
def api_response(status=True, message='', data=None, http_code=200):
    if data is None:
        data = {}
    return JsonResponse({
        "status": status,
        "http_code": http_code,
        "message": message,
        "data": data
    }, status=http_code, safe=False);

API_KEY = 'VzQ1Q2ZvVTZ5bWttMjc3MmMzbFM5eTlmMmJ2Q3NhSzFLZVJETmJhcA=='
api_headers = {
  'X-CSCAPI-KEY': API_KEY
}
# Get countries
def get_countries(request):
    res = requests.get("https://api.countrystatecity.in/v1/countries", headers=api_headers)
    if res.status_code == 200:
        return api_response(True, 'Countries fetched', {"countries": res.json()}, 200);
    return api_response(False, 'Failed to fetch countries', {}, res.status_code);

# Get states for a selected country
@csrf_exempt
def get_states(request):
    body = json.loads(request.body)
    country = body.get('country')
    country_code = body.get('country_code')
    url = "https://api.countrystatecity.in/v1/countries/"+country_code+"/states"
    res = requests.get(url, headers=api_headers)
    if res.status_code == 200:
        return api_response(True, 'States fetched', {"states": res.json()}, res.status_code);
    return api_response(False, 'Failed to fetch states', {}, res.status_code);

# Get cities for a selected country and state
@csrf_exempt
def get_cities(request):
    body = json.loads(request.body)
    country_code = body.get('country_code')  # e.g. 'IN'
    state_code = body.get('state_code')      # e.g. 'MP'
    url = "https://api.countrystatecity.in/v1/countries/"+country_code+"/states/"+state_code+"/cities"
    res = requests.get(url, headers=api_headers)
    if res.status_code == 200:
        return api_response(True, 'cities fetched', {"cities": res.json()}, res.status_code);
    return api_response(False, 'Failed to fetch cities', {}, res.status_code);
