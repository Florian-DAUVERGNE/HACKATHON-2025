import pandas as pd
import requests
import json
from airflow.models import Variable
from collections import defaultdict
from bs4 import BeautifulSoup
from datetime import datetime
import os
import ast
import io
import ollama
from ollama import generate
from fpdf import FPDF

from gtts import gTTS

API_KEY = Variable.get("SNCF_API")
URL = 'https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2'

global_data = None

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
    df_current_v2_merged = pd.merge(df_current_filtered, df_v2, on=['id'], how='left')
    return df_current_v2_merged,df_preview_filtered

def get_number_of_traveler_validations():
    df_current_v2_merged,df_preview_filtered = make_disruptions_pag_DF()
    n_validation = pd.read_csv(get_files_directory()+"data/nb_vald_par_arret_jour.csv", sep=";", encoding="utf-8")
    print(n_validation.iloc[177:183].head(10))
    print(df_current_v2_merged.head())
    # Filtrer les données pour une date spécifique
    #aujourd_hui = datetime.now()
    aujourd_hui = datetime(2025, 3, 27)  # Exemple de date spécifique
    yearFormat = aujourd_hui.strftime('%Y-%m-%d') 
    lastYearToday = (aujourd_hui.replace(year=aujourd_hui.year - 1)).strftime('%Y-%m-%d')
    # Convertir les noms des arrêts en minuscules
    n_validation['libelle_arret'] = n_validation['libelle_arret'].str.lower()
    numb = 0
    stations_valid_day = []
    # Filtrer les données pour la date spécifique
    filtered_validation = n_validation[n_validation['jour'] == lastYearToday]

    # Vérifier les arrêts dans df_current_v2_merged
    for stop_points in df_current_v2_merged['stop_points']:
        for stop in stop_points:
            if stop.lower() in filtered_validation['libelle_arret'].values:
                # Récupérer le nombre de validations correspondant
                nb_vald = filtered_validation.loc[filtered_validation['libelle_arret'] == stop.lower(), 'nb_vald'].values
                if len(nb_vald) > 0:
                    stations_valid_day.append(f"{stop} {nb_vald[0]}")
                    numb += 1

    print(numb)
    print(stations_valid_day)
    # Convertir les colonnes 'begin' et 'end' en datetime si ce n'est pas déjà fait
    df_current_v2_merged['begin'] = pd.to_datetime(df_current_v2_merged['begin'], format='%Y%m%dT%H%M%S', errors='coerce')
    df_current_v2_merged['end'] = pd.to_datetime(df_current_v2_merged['end'], format='%Y%m%dT%H%M%S', errors='coerce')

    # Ajouter une colonne 'total_valid_imp' initialisée à 0
    df_current_v2_merged['total_valid_imp'] = 0

    # Filtrer les données pour la date spécifique
    filtered_validation = n_validation[n_validation['jour'] == lastYearToday]

    # Parcourir chaque ligne de df_current_v2_merged
    for index, row in df_current_v2_merged.iterrows():
        stop_points = row['stop_points']  # Liste des arrêts impactés
        begin = row['begin']
        end = row['end']
        
        # Vérifier que begin et end sont valides
        if pd.notnull(begin) and pd.notnull(end):
            # Calculer la durée en heures entre begin et end
            duration_hours = (end - begin).total_seconds() / 3600
            
            # Filtrer les validations pour les arrêts impactés
            total_validations = 0
            for stop in stop_points:
                stop_lower = stop.lower()  # Convertir en minuscules pour correspondance
                if stop_lower in filtered_validation['libelle_arret'].str.lower().values:
                    # Récupérer le nombre de validations correspondant
                    nb_vald = filtered_validation.loc[filtered_validation['libelle_arret'].str.lower() == stop_lower, 'nb_vald'].values
                    if len(nb_vald) > 0:
                        total_validations += nb_vald[0]
            
            # Réduire le total des validations en fonction de la durée
            # Supposons que les validations sont réparties uniformément sur 24 heures
            impacted_validations = total_validations * (duration_hours / 24)
            
            # Ajouter le résultat dans la colonne 'total_valid_imp'
            df_current_v2_merged.at[index, 'total_valid_imp'] = round(impacted_validations)

    # Afficher un aperçu du DataFrame mis à jour
   # print(df_current_v2_merged[['id' ,'stop_points', 'begin', 'end', 'total_valid_imp']].head())

    return df_current_v2_merged,df_preview_filtered

def clean_text(text):
    """
    Fonction pour nettoyer le texte en supprimant les espaces inutiles autour des signes de ponctuation.
    """
    text = text.replace(" : ", ": ")  # Corriger les espaces avant les deux-points
    text = text.replace(" ,", ",")  # Supprimer les espaces avant les virgules
    text = text.strip()  # Enlever les espaces au début et à la fin du texte
    return text


def clean_and_describe_csv(file_path, title):
    # Lire le fichier CSV
    df = pd.read_csv(file_path)
    df = df.fillna(" ")  # Remplacer les NaN par une chaîne vide
    # Nettoyer les colonnes
    df['tags'] = df['tags'].apply(ast.literal_eval)
    df['stop_points'] = df['stop_points'].apply(ast.literal_eval)

    # Nettoyer la colonne 'text' si elle existe
    if 'text' in df.columns:
        df['text'] = df['text'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text())

    descriptions = [title]
    for index, row in df.iterrows():
        # Construire la description de la perturbation
        description = (
            f"Perturbation {index + 1}: Début: {row['begin']} à Fin: {row['end']}, Perturbation ID : {row['id']} "
            f"(dernière mise à jour: {row['lastUpdate']})\n"
            f"Cause: {row['cause']}. Sévérité : {row.get('severity_text', 'non spécifiée')}. "
            f"Tag: {', '.join(row['tags'])}. Title : {row['title']}. "
            f"Message: {row['message']}. Message court : {row['shortMessage']}. "
            f"Points d'arrêt affectés: {', '.join(row['stop_points'])}. "
            f"Nom: {row['name']}. Mode: {row['mode']}. "
        )
        
        # Ajouter des informations optionnelles si elles existent
        if 'status' in row:
            description += f"Statut perturbation: {row['status']}. "
        if 'priority' in row:
            description += f"Niveau de priorité: {row['priority']}. "
        if 'effect' in row:
            description += f"Effet: {row['effect']}. "
        if 'text' in row:
            description += f"Texte: {row['text']}. "
        if 'total_valid_imp' in row:
            description += f"Voyageurs impactés : {'valeur inconnue' if row['total_valid_imp'] == 0 else row['total_valid_imp']}."        

        # Appliquer le nettoyage du texte pour enlever les espaces inutiles
        description = clean_text(description)
        
        descriptions.append(description)
        descriptions.append("")  # Ajouter une ligne vide entre chaque perturbation
    return descriptions

def make_final_df():
    df_current_v2_merged,df_preview_filtered = get_number_of_traveler_validations()
    df_current_v2_merged.to_csv(get_files_directory()+'df_current_final.csv', index = False)
    df_preview_filtered.to_csv(get_files_directory()+'df_preview_final.csv', index = False)

    # Traiter chaque CSV séparément avec des titres distinctifs
    file_paths = [get_files_directory()+'df_current_final.csv', get_files_directory()+'df_preview_final.csv']
    titles = ["Perturbations en cours :", "Perturbations à venir :"]

    descriptions_list = [clean_and_describe_csv(file_path, title) for file_path, title in zip(file_paths, titles)]
    
    # Réunir les descriptions des deux CSV dans une seule variable
    combined_descriptions = "\n\n".join(["\n".join(descriptions) for descriptions in descriptions_list])

    # Afficher les descriptions combinées
    print("Descriptions combinées des deux CSV :\n")
    print(combined_descriptions)

    text_file = open(get_files_directory()+"combined_descriptions.txt", "w")
    text_file.write(combined_descriptions)
    text_file.close()
    return 0

def make_note():
    with open(get_files_directory()+'combined_descriptions.txt', 'r') as file:
        combined_descriptions = file.read()
    instruction_prompt = """Tu es un assistant qui rédige des synthèses humaines à partir de données structurées sur des perturbations ferroviaires. Pour chaque perturbation rédige un paragraphe clair, naturel et factuel en suivant les indications ci-dessous:
1. Commence par le nom de la ligne ou du mode de transport.
2. Indique la cause de la perturbation (par exemple, "en raison de travaux").
3. Précise la durée de l'interruption, avec les dates de début et de fin.
4. Mentionne si un service de remplacement est mis en place (par exemple, "bus de remplacement").
5. Cites les points d'arrêt affectés.
6. Mentionne le nombre de voyageurs impactés.
7. Indique la sévérité de la perturbation (par exemple, "bloquante" ou "perturbée").
8. Indique le niveau de priorité et le statut de la perturbation.
9. Le statut de la perturbation (now, new, finished).
10.Précise le statut de chaque perturbation si disponible.
11. Génère la note uniquement en français.
12.Sépare les perturbations qui sont en cours et à venir dans deux blocs distincts.
13.La note doit être sous la forme d'une annonce parlée et dans un ton professionnel.
"""
    perturbation = combined_descriptions
    prompt = instruction_prompt + "\n\nvoici les perturbations :\n\n" + perturbation

    print(prompt)

    OLLAMA_SERVER = 'http://192.168.1.26:11434'

    response = requests.post(
        f"{OLLAMA_SERVER}/api/generate",
        json={"model": "mistral:7b-instruct", "prompt": prompt,"stream": False}
    )

    print(response.json()['response'])

    # Sample text you want to save to PDF (this should already be UTF-8)
    text = response.json()['response']

    # Create a PDF instance
    pdf = FPDF()

    # Add a page to the PDF
    pdf.add_page()

    # Set font for the PDF (you can use a TrueType font that supports UTF-8)
    pdf.add_font("Arial", "", get_files_directory() +"Arial.ttf", uni=True)
    pdf.set_font("Arial", size=12)

    # Add the text to the PDF
    pdf.multi_cell(0, 10, text)

    # Save the PDF to a file
    pdf.output(get_files_directory() + "rapport.pdf")

    # Langue (par exemple, 'fr' pour le français)
    language = 'fr'

    # Conversion du texte en audio
    speech = gTTS(text=text, lang=language, slow=False)
    speech.save(get_files_directory() +"output.mp3")

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
    with open(get_files_directory() + "rapport.pdf", "rb") as f:
        pdf_data = f.read()
        msg.add_attachment(pdf_data, maintype="application", subtype="pdf", filename="document.pdf")
    
    #Attach MP3
    with open(get_files_directory() + "output.mp3", "rb") as f:
        mp3_data = f.read()
        msg.add_attachment(mp3_data, maintype="audio", subtype="mpeg", filename="audio.mp3")

    #Send email via SMTP
    with smtplib.SMTP("192.168.1.26", 2500) as smtp:
        #smtp.starttls()
        #smtp.login(sender_email, sender_password)
        smtp.send_message(msg)

    print("Email sent!")