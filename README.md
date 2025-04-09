<h1 align="center">
  <br>
    <img src="assets/LOGO_HACKATHON_2025.png" alt="NeuraPrice Logo" style="border-radius: 20px;">
  <br>
</h1>

<h1 align="center">
  <br>
  <img src="assets/L.I.S.A.png " alt="NeuraPrice Logo" width="200">
  <br>
  L.I.S.A. : Logiciel Intelligent pour la Surveillance des Alertes
  <br>
</h1>

<p align="center">
  • <a href="#description-du-projet">Description</a>
  • <a href="#structure-du-projet">Structure</a>
  • <a href="#technologies-utilisées">Technologies</a>
  • <a href="#prérequis">Prérequis</a>
  • <a href="#installation">Installation</a>
</p>




## Description du projet
Ce projet a été développé dans le cadre du Hackathon 2025. Il s'agit d'une solution innovante qui intègre plusieurs technologies modernes pour synthétiser les incidents du traffique en temps réel.

## Structure du projet
```
├── dags/               # DAGs Apache Airflow pour l'orchestration des tâches
├── files/             # Fichiers de données et ressources
├── notebook/          # Notebooks Jupyter pour l'analyse et le développement
├── tests/             # Tests unitaires et d'intégration
└── plugins/           # Plugins personnalisés
```

## Technologies utilisées
- Apache Airflow pour l'orchestration des workflows
- Docker pour la conteneurisation
- Python pour le développement backend
- Modèles de langage (Mistral)
- Text-to-Speech (TTS)

## Prérequis
- Python >3.11
- Docker
- Apache Airflow
- Un serveur SMTP

## Installation
1. Cloner le repository :
```bash
git clone https://github.com/Florian-DAUVERGNE/HACKATHON-2025.git
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

3. Configurer les variables d'environnement :
- Copier le fichier `.env.example` vers `.env`
- Remplir les variables nécessaires

4. Lancer le projet :
```bash
astro dev start
```

## Licence
[Spécifier la licence]

## Contact
[Informations de contact de l'équipe]
