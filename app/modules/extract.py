from FlightRadar24.api import FlightRadar24API
from FlightRadar24.errors import CloudflareError
from pyspark.sql.types import StructType, StructField, StringType
from collections import defaultdict
from tqdm import tqdm
import time
from utils import *

fr_api = FlightRadar24API()


def extract():
    # Extraction des données
    flights = fr_api.get_flights()
    airports = fr_api.get_airports()
    airlines = fr_api.get_airlines()

    schema = StructType(
        [
            StructField("latitude", StringType(), nullable=True),
            StructField("longitude", StringType(), nullable=True),
            StructField("altitude", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("icao", StringType(), nullable=True),
            StructField("iata", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]
    )
    flights_df = spark.createDataFrame(flights)
    airports_df = spark.createDataFrame(airports, schema=schema)
    airlines_df = spark.createDataFrame(airlines)

    # Récupérer des données de vol détaillées
    flights_detailed, _ = fetch_flight_details(flights_df)
    flights_detailed_df = extract_flight_details(flights_detailed)

    # Sauvegarde
    save_dataframe(flights_df, "ingest", "Flights")
    save_dataframe(airports_df, "ingest", "Airports")
    save_dataframe(airlines_df, "ingest", "Airlines")
    save_dataframe(flights_detailed_df, "ingest", "Flights_detailed")

    return flights_df, airports_df, airlines_df, flights_detailed_df


def fetch_flight_details(flights, max_retries=5, base_delay=2):
    flights_detailed = []
    flights_without_details = []  # Liste séparée pour les vols sans détails

    # Envelopper la liste des vols avec tqdm pour une barre de progression
    for flight in tqdm(flights, desc="Fetching flight details", unit="flight"):
        retries = 0
        fetched = (
            False  # Indicateur permettant de vérifier si les détails ont été récupérés
        )
        while retries < max_retries:
            try:
                flight_details = fr_api.get_flight_details(flight)
                flight.set_flight_details(flight_details)
                flights_detailed.append(flight)
                fetched = True
                break
            except (CloudflareError, ConnectionError):
                retries += 1
                sleep_time = base_delay**retries  # Exponential backoff
                time.sleep(sleep_time)

        # Si nous atteignons le nombre maximum de tentatives sans obtenir de détails, nous ajoutons le vol à la liste flights_without_details.
        if not fetched:
            flights_without_details.append(flight)

    return flights_detailed, flights_without_details


def extract_flight_details(flights_detailed):
    # Convertir les objets de vol en dictionnaires
    flights_as_dicts = [flight.__dict__ for flight in flights_detailed]

    # Détecter les types incohérents
    type_dict = defaultdict(set)
    for flight_dict in flights_as_dicts:
        for key, value in flight_dict.items():
            type_dict[key].add(type(value))

    # Identifier les clés dont les types sont incohérents
    inconsistent_keys = {key for key, types in type_dict.items() if len(types) > 1}

    # Harmoniser les types et convertir tout en chaîne pour les clés incohérentes
    for flight_dict in flights_as_dicts:
        for key in inconsistent_keys:
            flight_dict[key] = str(flight_dict[key])

    # Créer le DataFrame après harmonisation
    flights_detailed_df = spark.createDataFrame(flights_as_dicts)

    return flights_detailed_df
