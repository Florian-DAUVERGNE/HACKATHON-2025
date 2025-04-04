import pandas as pd
import requests
import json
from airflow.models import Variable

API_KEY = Variable.get("SNCF_API")

def get_dataset():

    URL = 'https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2'
    
    headers = {
        "Accept": "application/json",
        "apikey": API_KEY
    }

    response = requests.get(URL, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data, indent=2))
    else:
        print(f"Erreur {response.status_code}: {response.text}")