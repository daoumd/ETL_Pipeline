import logging
from utils import save_dataframe


def load(transformed_results):
    """
    Sauvegarde les résultats transformés dans des fichiers CSV.

    Arguments:
    - transformed_results: Dictionnaire contenant tous les DataFrames transformés.

    Retour:
    - Liste des chemins où les DataFrames ont été sauvegardés.
    """

    saved_paths = []

    for key, df in transformed_results.items():
        path = save_dataframe(df, "result", key, format="csv")
        saved_paths.append(path)
        logging.info(f"Résultats pour {key} sauvegardés dans {path}")

        # Afficher un aperçu des résultats pour confirmation
        logging.info(f"Aperçu des résultats pour {key}:\n{df.show(5)}")

    return saved_paths
