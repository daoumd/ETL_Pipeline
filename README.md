# FlightRadar24 ETL Pipeline
Le présent projet implémente un pipeline ETL pour traiter les données de l'API FlightRadar24, qui répertorie l'ensemble des vols aériens, aéroports, et compagnies aériennes mondiales.

### Structure du Projet
Le projet contient :

Jupyter Notebook : Il sert de point de départ pour explorer et tester le pipeline ETL étape par étape. Il offre également une documentation détaillée des différentes étapes et méthodes utilisées.

Code Modulaire : Le pipeline ETL a été conçu de manière modulaire, divisé en plusieurs fichiers pour une meilleure organisation et lisibilité. Chaque fichier se concentre sur une tâche spécifique, assurant ainsi la séparation des préoccupations et une maintenance plus aisée.

### Architecture idéal
Exemple d'architecture idéal

#### API FlightRadar24:
Source principale des données sur le trafic aérien en temps réel.

#### Apache Airflow:
Orchestre et planifie l'exécution du pipeline ETL, avec une interface visuelle pour le suivi.

#### Apache Spark:
Assure le traitement distribué des données, permettant nettoyage, transformation et préparation pour le stockage.

#### Amazon S3/HDFS:
Stockage final des données traitées, offrant rapidité, évolutivité et fiabilité.

#### ELK Stack:
Centralise et visualise les journaux pour le monitoring en temps réel, garantissant une réponse rapide aux problèmes.


![Schema drawio](https://github.com/daoumd/ETL_Pipeline/assets/140932918/94001942-06ab-4528-9df0-54aff9492def)
