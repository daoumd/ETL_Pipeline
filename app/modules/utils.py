import os
from datetime import datetime
from pyspark.sql import SparkSession

# Initialisation des paramètres d'environnement
os.environ["JAVA_HOME"] = "C:\Java"
os.environ["SPARK_HOME"] = "C:\Spark"

spark = (
    SparkSession.builder.appName("FlightRadar24 ETL Pipeline")
    .master("local[*]")
    .getOrCreate()
)


def save_dataframe(df, base_folder, data_type, format="parquet"):
    """
    Sauvegarde un DataFrame Spark dans un dossier spécifique avec une nomenclature horodatée.

    Arguments:
    - df: DataFrame Spark à sauvegarder.
    - base_folder: Dossier principal (par exemple, 'ingest', 'cleaned', 'result').
    - data_type: Type de données (par exemple, 'Flights', 'Airports', 'Airlines').
    - format: Format de sauvegarde (par défaut, 'parquet').
    """

    # Obtenir l'horodatage actuel
    current_time = datetime.now()
    year_month = current_time.strftime("%Y-%m")
    day = current_time.strftime("%Y-%m-%d")
    timestamp = current_time.strftime("%Y%m%d%H%M%S")

    # Créer le chemin horodaté
    base_path = f"/data/{base_folder}/{data_type}"
    data_path = os.path.join(
        base_path, year_month, day, f"{data_type.lower()}_{timestamp}.{format}"
    )

    # Sauvegarder le DataFrame
    if format == "parquet":
        df.write.parquet(data_path)
    elif format == "csv":
        df.write.csv(data_path)

    return data_path


def read_dataframe(base_folder, data_type, specific_date=None, format="parquet"):
    """
    Lit un DataFrame Spark depuis un dossier spécifique avec une nomenclature horodatée.

    Arguments:
    - base_folder: Dossier principal (par exemple, 'ingest', 'cleaned', 'result').
    - data_type: Type de données (par exemple, 'Flights', 'Airports', 'Airlines').
    - specific_date: Date spécifique à partir de laquelle lire le DataFrame (par défaut, None pour la date actuelle).
    - format: Format de sauvegarde (par défaut, 'parquet').

    Retour:
    - DataFrame Spark chargé depuis le chemin spécifié.
    """

    # Si aucune date spécifique n'est fournie, utilisez l'horodatage actuel
    if specific_date is None:
        current_time = datetime.now()
        year_month = current_time.strftime("%Y-%m")
        day = current_time.strftime("%Y-%m-%d")
    else:
        year_month = specific_date.strftime("%Y-%m")
        day = specific_date.strftime("%Y-%m-%d")

    # Créer le chemin horodaté
    base_path = f"/data/{base_folder}/{data_type}"
    data_path = os.path.join(base_path, year_month, day)

    # Charger le DataFrame depuis le chemin spécifié
    if format == "parquet":
        df = spark.read.parquet(data_path)
    elif format == "csv":
        df = spark.read.csv(data_path)

    return df
