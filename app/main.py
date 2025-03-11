import os
import mysql.connector
from load_raw import loading_raw
from load_silver import loading_silver
from load_gold import loading_gold

# Récupérer les variables d'environnement
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "raw")
LOCALSTACK_URL = os.getenv("LOCALSTACK_URL", "http://localstack:4566")

DB_SILVER_NAME = "silver"
DB_GOLD_NAME = "gold"
DB_HOST = "mysql"
DB_USER = "root"
DB_PASSWORD = "root"

def create_gold_database():

    # Connexion au serveur MySQL (sans spécifier de base de données pour le moment)
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )

    """
    Crée la base de données 'gold' si elle n'existe pas.
    """
    try:
        cursor = connection.cursor()

        # Créer la base de données si elle n'existe pas
        cursor.execute("CREATE DATABASE IF NOT EXISTS {0};".format(DB_GOLD_NAME))
        print("Base de données 'gold' créée ou déjà existante.")

    except Exception as e:
        print(f"Erreur lors de la création de la base de données 'gold' : {e}")

    finally:
        cursor.close()

    # Fermer la connexion
    connection.close()


def main():
    create_gold_database()
    loading_raw(S3_BUCKET_NAME, LOCALSTACK_URL)
    loading_silver(DB_HOST, DB_USER, DB_PASSWORD, LOCALSTACK_URL, S3_BUCKET_NAME, DB_SILVER_NAME)
    loading_gold(DB_HOST, DB_USER, DB_PASSWORD, DB_GOLD_NAME, DB_SILVER_NAME)

if __name__ == "__main__":
    main()
