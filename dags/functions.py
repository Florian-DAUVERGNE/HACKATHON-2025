import pandas as pd
import requests
import json
from airflow.models import Variable
from collections import defaultdict
from bs4 import BeautifulSoup
from datetime import datetime
import os
import io


API_KEY = Variable.get("SNCF_API")
URL = 'https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2'

global_data = None

def get_files_directory():
    current_directory = os.getcwd() + "/files/"
    print(f"Les données sont stockées : {current_directory}")
    return str(current_directory)

def get_files_directory():
    current_directory = os.getcwd() + "/files/"
    print(f"Les données sont stockées : {current_directory}")
    return str(current_directory)

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

def filtrer_evenements_en_cours(df, col_debut="begin", col_fin="end", now=None):
    """
    Filtre les événements en cours à partir d'un DataFrame avec colonnes 'start' et 'end' (format 'YYYYMMDDTHHMMSS').

    :param df: DataFrame contenant les événements
    :param col_debut: nom de la colonne de début (par défaut "start")
    :param col_fin: nom de la colonne de fin (par défaut "end")
    :param now: datetime personnalisé (utile pour les tests), sinon datetime.now()
    :return: DataFrame filtré avec les événements en cours
    """
    df = df.copy()
    
    # Conversion des dates
    df[col_debut] = pd.to_datetime(df[col_debut], format="%Y%m%dT%H%M%S", errors='coerce')
    df[col_fin] = pd.to_datetime(df[col_fin], format="%Y%m%dT%H%M%S", errors='coerce')

    # Date/heure actuelle
    now = now or pd.Timestamp.now()

    # Filtrage des événements en cours
    df_current = df[(df[col_debut] <= now) & (df[col_fin] >= now)]
    
    return df_current

def filtrer_evenements_previsionnels(df, col_debut="begin", col_fin="end", now=None):
    """
    Filtre les événements dont le début est dans les 2 heures à venir.

    :param df: DataFrame contenant les événements
    :param col_debut: nom de la colonne de début
    :param col_fin: nom de la colonne de fin
    :param now: datetime personnalisé (utile pour les tests), sinon datetime.now()
    :return: DataFrame filtré avec les événements débutant dans les 2 prochaines heures
    """
    df = df.copy()
    
    # Conversion des dates
    df[col_debut] = pd.to_datetime(df[col_debut], format="%Y%m%dT%H%M%S", errors='coerce')
    df[col_fin] = pd.to_datetime(df[col_fin], format="%Y%m%dT%H%M%S", errors='coerce')

    # Date/heure actuelle
    now = now or pd.Timestamp.now()
    dans_2h = now + pd.Timedelta(hours=7)

    # Filtrage des événements qui commenceront dans les 2 prochaines heures
    df_preview = df[(df[col_debut] >= now) & (df[col_debut] <= dans_2h)]

    return df_preview


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

    df_disruptions = make_disruptions_DF()
    df_lines = make_lines_DF()
    
    df_current = df_current = filtrer_evenements_en_cours(df_disruptions)
    df_preview = filtrer_evenements_previsionnels(df_disruptions)


    df_current_merged = df_current.merge(df_lines, on='id', how='left')
    df_preview_merged = df_preview.merge(df_lines, on='id', how='left')

    # Supprimer valeurs NAN et converttir en str
    df_current_merged['message'] = df_current_merged['message'].astype(str).fillna('')

    # Appliquer la suppression des balises HTML à chaque message de la colonne 'message'
    df_current_merged['message'] = df_current_merged['message'].apply(
        lambda msg: BeautifulSoup(msg, "html.parser").get_text()
    )

    # Supprimer valeurs NAN et converttir en str
    df_preview_merged['message'] = df_preview_merged['message'].astype(str).fillna('')

    # Appliquer la suppression des balises HTML à chaque message de la colonne 'message'
    df_preview_merged['message'] = df_preview_merged['message'].apply(
        lambda msg: BeautifulSoup(msg, "html.parser").get_text()
    )

    return df_current_merged,df_preview_merged

def make_status():
    df_current_merged,df_preview_merged = make_combine_dataset()
    # Récupérer les ID des perturbations du précédent appel API
    # Par exemple, ces ID pourraient être stockés dans un fichier ou une base de données.
    previous_disruptions = set(df_current_merged['id'].tolist())

    # Simulons un nouvel appel API en modifiant df_merged pour représenter le nouvel état des perturbations
    # Ce dataframe (df_merged) serait mis à jour à chaque appel API avec de nouvelles données.
    new_disruptions = set(df_current_merged['id'].tolist())  # Ici on reprend la même liste, mais dans un vrai cas, ce serait mis à jour.

    # Ajouter la colonne 'status' en fonction des conditions
    df_current_merged['status'] = df_current_merged['id'].apply(
        lambda x: 'new' if x not in previous_disruptions else ('finished' if x not in new_disruptions else 'now')
    )

    # Afficher le DataFrame avec la nouvelle colonne 'status'
    print(df_current_merged.head())

def test():
    CSV_path = get_files_directory() + 'df_previous_merged.csv'
    df = pd.read_csv(CSV_path)
    print(df)

def send_mail():
    import smtplib
    from email.message import EmailMessage

    #Email details
    sender_email = "your_email@gmail.com"
    sender_password = "your_app_password"  # Use an app password if 2FA is enabled
    receiver_email = "receiver@example.com"
    subject = "Here is your PDF!"
    body = "Hi there,\n\nPlease find the attached PDF.\n\nBest regards. Sent by Airflow !" 

    #Create email
    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.set_content(body)

    #Send email via SMTP
    with smtplib.SMTP("192.168.1.26", 2500) as smtp:
        #smtp.starttls()
        #smtp.login(sender_email, sender_password)
        smtp.send_message(msg)

    print("Email sent!")