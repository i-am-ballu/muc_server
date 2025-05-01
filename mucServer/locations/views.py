from django.shortcuts import render

# Create your views here.
import requests
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json

# Get countries
def get_countries(request):
    res = requests.get("https://countriesnow.space/api/v0.1/countries/positions")
    if res.status_code == 200:
        data = res.json()
        countries = [c['name'] for c in data['data']]
        return JsonResponse({'countries': countries})
    return JsonResponse({'error': 'Unable to fetch countries'}, status=500)

# Get states for a selected country
@csrf_exempt
def get_states(request):
    body = json.loads(request.body)
    country = body.get('country')
    res = requests.post("https://countriesnow.space/api/v0.1/countries/states", json={"country": country})
    if res.status_code == 200:
        data = res.json()
        return JsonResponse({'states': data['data']['states']})
    return JsonResponse({'error': 'Unable to fetch states'}, status=500)

# Get cities for a selected country and state
@csrf_exempt
def get_cities(request):
    body = json.loads(request.body)
    country = body.get('country')
    state = body.get('state')
    res = requests.post("https://countriesnow.space/api/v0.1/countries/state/cities", json={"country": country, "state": state})
    if res.status_code == 200:
        data = res.json()
        return JsonResponse({'cities': data['data']})
    return JsonResponse({'error': 'Unable to fetch cities'}, status=500)
