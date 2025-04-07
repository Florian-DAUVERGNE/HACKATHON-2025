import pandas as pd
import requests
import json
from airflow.models import Variable
from collections import defaultdict
from bs4 import BeautifulSoup

API_KEY = Variable.get("SNCF_API")
URL = 'https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2'

global_data = None

def make_GLOBAL_API_request():
    global global_data
    headers = {
        "Accept": "application/json",
        "apikey": API_KEY
    }

    response = requests.get(URL, headers=headers)

    if response.status_code == 200:
        global_data = response.json()
    else:
        print(f"Erreur {response.status_code}: {response.text}")

def make_disruptions_DF():
    global global_data
    disruptions = global_data.get("disruptions", [])

    df_disruptions = pd.json_normalize(disruptions, 
                                    record_path='applicationPeriods', 
                                    meta=['id', 'lastUpdate', 'cause', 'severity', 'tags', 'title', 'message', 'shortMessage'], 
                                    sep=',', errors='ignore')
    return df_disruptions

def make_lines_DF():
    global global_data
    lines = global_data.get("lines", [])

    disruption_to_stops = defaultdict(set)

    disruption_line_info = {}

    for line in lines:
        for obj in line.get('impactedObjects', []):
            if obj['type'] == 'stop_point':
                stop_name = obj['name']
                for disruption_id in obj.get('disruptionIds', []):
                    disruption_to_stops[disruption_id].add(stop_name)
                    if disruption_id not in disruption_line_info:
                        disruption_line_info[disruption_id] = {
                            'name': line['name'],
                            'mode': line['mode']
                        }

    df_lines = pd.DataFrame([
        {
            'disruptionId': disruption_id,
            'stop_points': sorted(list(stops)),
            'name': disruption_line_info[disruption_id]['name'],
            'mode': disruption_line_info[disruption_id]['mode']
        }
        for disruption_id, stops in disruption_to_stops.items()
    ])

    return df_lines.rename(columns={'disruptionId': 'id'})

def make_combine_dataset():

    make_GLOBAL_API_request()

    df_disruption = make_disruptions_DF()
    df_lines = make_lines_DF()

    df_merged = df_disruption.merge(df_lines, on='id', how='left')
    df_merged['message'] = df_merged['message'].astype(str).fillna('')

    df_merged['message'] = df_merged['message'].apply(
        lambda msg: BeautifulSoup(msg, "html.parser").get_text()
    )
    print(df_merged.head())