from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import *


def most_active_airline(flights_detailed_enrichi_df, airlines_df):
    """
    Identifie la compagnie aérienne avec le plus grand nombre de vols en cours.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.
    - airlines_df: DataFrame des compagnies aériennes.

    Retour:
    - DataFrame contenant la compagnie aérienne avec le plus grand nombre de vols en cours.
    """

    # Grouper par IATA de la compagnie aérienne et compter le nombre de vols
    most_active_airline_df = (
        flights_detailed_enrichi_df.filter(F.col("airline_iata") != "N/A")
        .groupBy("airline_iata")
        .agg(F.count("*").alias("number_of_flights"))
        .orderBy(F.desc("number_of_flights"))
    )

    # Joindre avec airlines_df pour obtenir le nom de la compagnie aérienne
    result = (
        most_active_airline_df.join(
            airlines_df, most_active_airline_df.airline_iata == airlines_df.Code
        )
        .select("Name", "number_of_flights")
        .orderBy(F.desc("number_of_flights"))
    )

    return result


def top_regional_airline_per_continent(flights_detailed_enrichi_df):
    """
    Identifie pour chaque continent la compagnie aérienne avec le plus grand nombre de vols régionaux actifs.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.

    Retour:
    - DataFrame contenant la compagnie aérienne avec le plus grand nombre de vols régionaux pour chaque continent.
    """

    # Filtrer pour les vols régionaux
    regional_flights = flights_detailed_enrichi_df.filter(
        F.col("origin_continent") == F.col("destination_continent")
    )

    # Grouper par continent et IATA de la compagnie aérienne
    grouped_data = regional_flights.groupBy("origin_continent", "airline_iata").agg(
        F.count("*").alias("number_of_flights")
    )

    # Utiliser une fenêtre pour obtenir le rang pour chaque compagnie aérienne par continent
    windowSpec = Window.partitionBy("origin_continent").orderBy(
        F.desc("number_of_flights")
    )
    ranked_data = grouped_data.withColumn("rank", F.row_number().over(windowSpec))

    # Filtrer pour obtenir la compagnie aérienne avec le plus grand nombre de vols pour chaque continent
    top_airline_per_continent = (
        ranked_data.filter(F.col("rank") == 1).drop("rank").orderBy("origin_continent")
    )

    return top_airline_per_continent


def longest_ongoing_flight(flights_detailed_enrichi_df, airlines_df):
    """
    Identifie le vol en cours avec le trajet le plus long.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.
    - airlines_df: DataFrame des compagnies aériennes.

    Retour:
    - DataFrame contenant le vol en cours avec la durée de vol la plus longue.
    """

    # Joindre avec airlines_df pour obtenir le nom de la compagnie aérienne
    result = (
        flights_detailed_enrichi_df.join(
            airlines_df,
            flights_detailed_enrichi_df.airline_iata == airlines_df.Code,
            "left",
        )
        .orderBy(F.desc("estimated_flight_duration"))
        .select(
            "id",
            "estimated_flight_duration",
            "Name",
            "origin_airport_country_name",
            "destination_airport_country_name",
        )
    )

    return result.limit(1)


def average_flight_duration_per_continent(flights_detailed_enrichi_df):
    """
    Calcule la durée moyenne du vol pour chaque continent.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.

    Retour:
    - DataFrame contenant la durée moyenne du vol pour chaque continent.
    """

    # Filtrer les vols régionaux
    regional_flights = flights_detailed_enrichi_df.filter(
        F.col("origin_continent") == F.col("destination_continent")
    )

    # Recalculer la durée estimée du vol
    regional_flights = regional_flights.withColumn(
        "estimated_flight_duration",
        F.when(
            F.col("time_details.scheduled.arrival")
            > F.col("time_details.scheduled.departure"),
            F.col("time_details.scheduled.arrival")
            - F.col("time_details.scheduled.departure"),
        ).otherwise(None),
    )

    # Calculer la durée moyenne du vol pour chaque continent
    average_duration_per_continent = regional_flights.groupBy("origin_continent").agg(
        F.avg("estimated_flight_duration").alias("average_flight_duration")
    )

    return average_duration_per_continent


def most_active_aircraft_manufacturer(flights_detailed_enrichi_df):
    """
    Identifie le constructeur d'avions avec le plus de vols actifs.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.

    Retour:
    - DataFrame contenant le constructeur avec le plus de vols actifs.
    """

    result = (
        flights_detailed_enrichi_df.filter(F.col("aircraft_manufacturer") != "N/A")
        .groupBy("aircraft_manufacturer")
        .agg(F.count("*").alias("number_of_flights"))
        .orderBy(F.desc("number_of_flights"))
    )

    return result


def top3_aircrafts_per_country(flights_detailed_enrichi_df):
    """
    Identifie le top 3 des modèles d'avion en usage pour chaque pays de compagnie aérienne.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.

    Retour:
    - DataFrame contenant le top 3 des modèles d'avion pour chaque pays.
    """

    windowSpec = Window.partitionBy("aircraft_country").orderBy(F.desc("count"))

    top_aircrafts = (
        flights_detailed_enrichi_df.filter(F.col("aircraft_country").isNotNull())
        .groupBy("aircraft_country", "aircraft_model")
        .agg(F.count("*").alias("count"))
    )

    result = top_aircrafts.withColumn("rank", F.row_number().over(windowSpec)).filter(
        (F.col("rank") <= 3) & (F.col("aircraft_country").isNotNull())
    )

    return result


def airport_with_max_diff_departure_arrival(flights_detailed_enrichi_df, airports_df):
    """
    Identifie l'aéroport avec la plus grande différence entre le nombre de vols sortants et le nombre de vols entrants.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.
    - airports_df: DataFrame des aéroports.

    Retour:
    - DataFrame contenant l'aéroport avec la plus grande différence entre le nombre de vols sortants et le nombre de vols entrants.
    """

    departures = flights_detailed_enrichi_df.groupBy("origin_airport_iata").agg(
        F.count("*").alias("departures")
    )
    arrivals = flights_detailed_enrichi_df.groupBy("destination_airport_iata").agg(
        F.count("*").alias("arrivals")
    )

    difference_with_diff = (
        departures.join(
            arrivals,
            departures.origin_airport_iata == arrivals.destination_airport_iata,
            "outer",
        )
        .fillna(0)
        .withColumn("diff", F.abs(F.col("departures") - F.col("arrivals")))
    )

    # Joindre avec airports_df pour obtenir le nom de l'aéroport
    result_bonus = (
        difference_with_diff.join(
            airports_df, difference_with_diff.origin_airport_iata == airports_df.iata
        )
        .select("name", "departures", "arrivals", "diff")
        .orderBy(F.desc("diff"))
    )

    return result_bonus


def transform(flights_detailed_enrichi_df, airlines_df, airports_df):
    """
    Orchestre les différentes étapes de transformation des données.

    Arguments:
    - flights_detailed_enrichi_df: DataFrame des vols nettoyés et enrichis.
    - airlines_df: DataFrame des compagnies aériennes.
    - airports_df: DataFrame des aéroports.

    Retour:
    - Dictionnaire contenant tous les DataFrames transformés.
    """

    results = {}

    results["most_active_airline"] = most_active_airline(
        flights_detailed_enrichi_df, airlines_df
    )
    results["top_regional_airline_per_continent"] = top_regional_airline_per_continent(
        flights_detailed_enrichi_df
    )
    results["longest_ongoing_flight"] = longest_ongoing_flight(
        flights_detailed_enrichi_df, airlines_df
    )
    results[
        "average_flight_duration_per_continent"
    ] = average_flight_duration_per_continent(flights_detailed_enrichi_df)
    results["most_active_aircraft_manufacturer"] = most_active_aircraft_manufacturer(
        flights_detailed_enrichi_df
    )
    results["top3_aircrafts_per_country"] = top3_aircrafts_per_country(
        flights_detailed_enrichi_df
    )
    results[
        "airport_with_max_diff_departure_arrival"
    ] = airport_with_max_diff_departure_arrival(
        flights_detailed_enrichi_df, airports_df
    )

    return results
