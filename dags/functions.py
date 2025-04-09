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
    return df_current_merged,df_preview_merged

def save_to_csv():
    df_current_merged,df_preview_merged = make_status()
    CSV_path = get_files_directory() + 'df_previous_merged.csv'

    try:
        df_previous = pd.read_csv(CSV_path)
        print("Fichier précédent chargé depuis CSV.")
    except FileNotFoundError:
        df_previous = pd.DataFrame(columns=["id", "name", "mode", "begin", "end", "severity", "tags", "title", "message", "status"])
        print("Aucun fichier précédent trouvé, c’est le premier appel ?")
        
    df_current_merged.to_csv(f"df_previous_merged.csv", index=False)
    return df_previous,df_preview_merged

def make_hist():
    df_previous,df_preview_merged = save_to_csv()
    CSV_path = get_files_directory() + 'df_previous_merged.csv'
    # Ajouter la colonne 'status' pour les disruptions actuelles
    df_current = pd.read_csv(CSV_path)
    df_current["status"] = df_current["id"].apply(
        lambda x: "new" if x not in df_previous["id"].values else "now"
    )

    # Identifier les disruptions terminées ("finished")
    finished_ids = set(df_previous["id"]) - set(df_current["id"])
    df_finished = df_previous[df_previous["id"].isin(finished_ids)].copy()
    df_finished["status"] = "finished"  # Ajouter le statut "finished"
    df_current_histo = pd.concat([df_current, df_finished], ignore_index=True)
    return df_current_histo,df_preview_merged

def make_RF_DF():
    df_current_histo,df_preview_merged = make_hist()
    df_current_filtered = df_current_histo[df_current_histo['mode'].isin(['RapidTransit', 'LocalTrain', 'Tramway', 'Metro'])]
    df_preview_filtered = df_preview_merged[df_preview_merged['mode'].isin(['RapidTransit', 'LocalTrain', 'Tramway', 'Metro'])]
    return df_current_filtered,df_preview_filtered

def make_INFOMESSAGE_API_request():
    # URL de base de l'API
    base_url = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/line_reports"
    headers = {
        "apikey": API_KEY  # Remplace avec ta vraie clé API
    }

    # Pagination
    start_page = 0
    items_per_page = 25
    all_rows = []

    while True:
        params = {
            "start_page": start_page,
            "count": items_per_page
        }
        
        response = requests.get(base_url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Erreur {response.status_code} à la page {start_page}")
            break

        json_data = response.json()
        disruptions = json_data.get("disruptions", [])

        if not disruptions:
            break  # Plus de disruptions à parcourir

        # Traitement de chaque disruption
        for disruption in disruptions:
            html_message = next(
                (msg['text'] for msg in disruption.get('messages', [])
                if msg.get('channel', {}).get('content_type') == 'text/html'),
                None
            )
            
            row = {
                'priority': disruption['severity']['priority'],
                'effect': disruption['severity']['effect'],
                'severity_text': disruption['severity']['name'],
                #'begin': disruption['application_periods'][0]['begin'],
                #'end': disruption['application_periods'][0]['end'],
                'id': disruption['id'],
                'text': html_message
            }
            all_rows.append(row)

        print(f"Page {start_page} traitée avec {len(disruptions)} disruptions.")
        
        # Vérification fin de pagination
        pagination = json_data.get('pagination', {})
        total_result = pagination.get('total_result', 0)
        if (start_page + 1) * items_per_page >= total_result:
            break

        start_page += 1

    # Construction du DataFrame
    df_v2 = pd.DataFrame(all_rows)
    return df_v2


def make_disruptions_pag_DF():
    df_current_filtered,df_preview_filtered = make_RF_DF()
    df_v2 = make_INFOMESSAGE_API_request()
    print(type(df_current_filtered))
    df_current_v2_merged = pd.merge(df_current_filtered, df_v2, on=['id'], how='left')
    print(df_current_v2_merged['text'][0])



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

    #Attach PDF
    with open(get_files_directory() + "Répartition.pdf", "rb") as f:
        pdf_data = f.read()
        msg.add_attachment(pdf_data, maintype="application", subtype="pdf", filename="document.pdf")

    #Send email via SMTP
    with smtplib.SMTP("192.168.1.26", 2500) as smtp:
        #smtp.starttls()
        #smtp.login(sender_email, sender_password)
        smtp.send_message(msg)

    print("Email sent!")