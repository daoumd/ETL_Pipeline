import logging
from modules.extract import extract
from modules.clean import clean_data
from modules.transform import transform
from modules.load import load


def main():
    logging.info("Début du pipeline ETL.")

    # Extraction
    flights_df, airports_df, airlines_df, flights_detailed_df = extract()

    # Nettoyage
    flights_detailed_enrichi_df = clean_data(flights_detailed_df)

    # Transformation
    transformed_results = transform(
        flights_detailed_enrichi_df, airlines_df, airports_df
    )

    # Chargement
    load(transformed_results)

    logging.info("Fin du pipeline ETL.")


if __name__ == "__main__":
    main()
