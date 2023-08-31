from pyspark.sql.functions import col, when, udf, split
from pyspark.sql.types import StringType
from utils import *

from flydenity import Parser

parser = Parser()

# UDF pour corriger les noms de pays
from pycountry_convert import (
    country_name_to_country_alpha2,
    country_alpha2_to_continent_code,
)


# UDF pour corriger les noms de pays
@udf(StringType())
def correct_country_name(country_name):
    country_corrections = {
        "Democratic Republic Of The Congo": "Congo, The Democratic Republic of the",
        "Antigua And Barbuda": "Antigua and Barbuda",
        "Reunion": "Réunion",
        "Curacao": "Curaçao",
        "Saint Vincent And The Grenadines": "Saint Vincent and the Grenadines",
    }
    return country_corrections.get(country_name, country_name)


# UDF pour convertir le nom du pays en code de continent
continents = {
    "NA": "North America",
    "SA": "South America",
    "AS": "Asia",
    "OC": "Australia",
    "AF": "Africa",
    "EU": "Europe",
}


def country_to_continent(country):
    try:
        alpha2 = country_name_to_country_alpha2(country)
        continent_code = country_alpha2_to_continent_code(alpha2)
        return continents[continent_code]
    except:
        return None


convert_to_continent_udf = udf(country_to_continent, StringType())


# UDF pour extraire le pays de l'aéronef à partir de son immatriculation
def registration_to_country(registration):
    try:
        return parser.parse(registration)["nation"]
    except:
        return None


registration_to_country_udf = udf(lambda x: registration_to_country(x), StringType())


def clean_data(flights_detailed_df):
    """
    Effectue une série d'opérations de nettoyage et d'enrichissement sur le DataFrame flights_detailed_df.

    Arguments:
    - flights_detailed_df: DataFrame contenant les détails des vols.

    Retour:
    - DataFrame nettoyé et enrichi.
    """

    # Nettoyage initial
    flights_detailed_df = flights_detailed_df.withColumn(
        "aircraft_manufacturer", split(col("aircraft_model"), " ")[0]
    )
    flights_detailed_df = flights_detailed_df.withColumn(
        "origin_airport_country_name",
        correct_country_name(col("origin_airport_country_name")),
    )
    flights_detailed_df = flights_detailed_df.withColumn(
        "destination_airport_country_name",
        correct_country_name(col("destination_airport_country_name")),
    )

    # Enrichissement
    flights_detailed_df = flights_detailed_df.withColumn(
        "origin_continent", convert_to_continent_udf(col("origin_airport_country_name"))
    )
    flights_detailed_df = flights_detailed_df.withColumn(
        "destination_continent",
        convert_to_continent_udf(col("destination_airport_country_name")),
    )
    flights_detailed_df = flights_detailed_df.withColumn(
        "estimated_flight_duration",
        when(
            col("time_details.scheduled.arrival").isNotNull()
            & col("time_details.scheduled.departure").isNotNull(),
            col("time_details.scheduled.arrival")
            - col("time_details.scheduled.departure"),
        ).otherwise(None),
    )
    flights_detailed_df = flights_detailed_df.withColumn(
        "aircraft_country", registration_to_country_udf(col("registration"))
    )

    # Sélection de colonnes
    columns_to_keep = [
        "id",
        "airline_iata",
        "origin_airport_country_code",
        "destination_airport_country_code",
        "latitude",
        "longitude",
        "destination_airport_latitude",
        "destination_airport_longitude",
        "aircraft_manufacturer",
        "aircraft_model",
        "registration",
        "time_details",
        "origin_airport_iata",
        "destination_airport_iata",
        "origin_airport_country_name",
        "destination_airport_country_name",
        "origin_continent",
        "destination_continent",
        "estimated_flight_duration",
        "aircraft_country",
    ]
    flights_detailed_enrichi_df = flights_detailed_df.select(columns_to_keep)
    flights_detailed_enrichi_df = flights_detailed_enrichi_df.dropna(
        subset=[
            "airline_iata",
            "origin_airport_country_code",
            "destination_airport_country_code",
            "aircraft_manufacturer",
        ]
    )

    # Enregistrement du DataFrame nettoyé
    save_dataframe(flights_detailed_enrichi_df, "enhanced", "Flights_detailed")

    return flights_detailed_enrichi_df
