import argparse
import boto3
from mysql.connector import Error
import pandas as pd
from io import StringIO
from utils import create_mysql_connection


def get_data_from_raw(endpoint_url, bucket_name, file_name="data.csv"):
    """Récupère les données depuis le bucket raw."""
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=endpoint_url,
                                 aws_access_key_id="test",
                                 aws_secret_access_key="test",
                                 region_name="us-east-1")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        print(f"Erreur lors de la récupération des données depuis S3: {e}")
        return None

def clean_data(content):
    """Nettoie les données (suppression des doublons et des lignes vides)."""
    # Convertir le contenu en DataFrame
    # Schéma typé avec les colonnes et leurs types
    schema = {
        "id": str,
        "vendor_id": int,
        "passenger_count": int,
        "pickup_longitude": float,
        "pickup_latitude": float,
        "dropoff_longitude": float,
        "dropoff_latitude": float,
        "store_and_fwd_flag": str,
        "trip_duration": float,
    }
    df = pd.read_csv(StringIO(content), dtype=schema, parse_dates=["pickup_datetime", "dropoff_datetime"])

    # Supprimer les lignes vides
    df = df.dropna()

    # Supprimer les doublons
    df = df.drop_duplicates()

    return df

def create_table(connection):
    """Crée la table texts si elle n'existe pas."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS taxi_trip (
                id VARCHAR(255) PRIMARY KEY, -- Définir une longueur pour le VARCHAR
                vendor_id INT NOT NULL,
                pickup_datetime TIMESTAMP NOT NULL,
                dropoff_datetime TIMESTAMP NOT NULL,
                passenger_count INT NOT NULL CHECK (passenger_count >= 0), -- Vérification pour un nombre valide
                pickup_longitude FLOAT NOT NULL CHECK (pickup_longitude BETWEEN -180 AND 180), -- Longitude doit être valide
                pickup_latitude FLOAT NOT NULL CHECK (pickup_latitude BETWEEN -90 AND 90), -- Latitude doit être valide
                dropoff_longitude FLOAT NOT NULL CHECK (dropoff_longitude BETWEEN -180 AND 180),
                dropoff_latitude FLOAT NOT NULL CHECK (dropoff_latitude BETWEEN -90 AND 90),
                store_and_fwd_flag CHAR(1) NOT NULL CHECK (store_and_fwd_flag IN ('Y', 'N')), -- Limiter à 'Y' ou 'N'
                trip_duration FLOAT NOT NULL CHECK (trip_duration >= 0) -- Durée positive ou nulle
);

        """)
        connection.commit()
    except Error as e:
        print(f"Erreur lors de la création de la table: {e}")

def insert_data(connection, df, batch_size=1000):
    """Insère les données dans la table taxi_trip par lots."""
    try:
        cursor = connection.cursor()

        # Préparer la requête d'insertion
        insert_query = """
        INSERT INTO taxi_trip (
            id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
            pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, 
            store_and_fwd_flag, trip_duration
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Diviser le dataframe en lots de taille batch_size
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            values = [
                (
                    row['id'], row['vendor_id'], row['pickup_datetime'], row['dropoff_datetime'],
                    row['passenger_count'], row['pickup_longitude'], row['pickup_latitude'],
                    row['dropoff_longitude'], row['dropoff_latitude'], row['store_and_fwd_flag'],
                    row['trip_duration']
                )
                for _, row in batch.iterrows()
            ]

            # Exécuter l'insertion pour chaque lot
            cursor.executemany(insert_query, values)
            connection.commit()
            print(f"Inséré {len(values)} lignes du lot {i // batch_size + 1}")

        print("Insertion terminée.")
    except Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")

def validate_data(connection):
    """Valide les données insérées avec des requêtes SQL."""
    try:
        cursor = connection.cursor()

        # Compte total des lignes
        cursor.execute("SELECT COUNT(*) FROM taxi_trip")
        total_count = cursor.fetchone()[0]
        print(f"Nombre total de lignes: {total_count}")

        # Compte des lignes non vides
        cursor.execute("SELECT COUNT(*) FROM taxi_trip WHERE store_and_fwd_flag='N'")
        store_and_fwd_flag_N_count = cursor.fetchone()[0]
        print(f"Nombre de lignes store_and_fwd_flag=='N': {store_and_fwd_flag_N_count}")

        # Exemple de quelques lignes
        cursor.execute("SELECT id,vendor_id, pickup_datetime FROM taxi_trip LIMIT 5")
        print("\nExemple de 5 premières lignes:")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, vendor_id: {row[1]}, pickup_datetime: {row[2]}")

    except Error as e:
        print(f"Erreur lors de la validation des données: {e}")

def loading_silver(DB_HOST, DB_USER, DB_PASSWORD, ENDPOINT_URL, S3_BUCKET_NAME, DB_NAME):

    # Récupérer les données depuis raw
    print("Récupération des données depuis le bucket raw...")
    content = get_data_from_raw(ENDPOINT_URL, S3_BUCKET_NAME)
    if content is None:
        return

    # Nettoyer les données
    print("Nettoyage des données...")
    df = clean_data(content)

    # Connexion à MySQL
    print("Connexion à MySQL...")
    connection = create_mysql_connection(
        DB_HOST,
        DB_USER,
        DB_PASSWORD,
        DB_NAME
    )
    if connection is None:
        return

    # Créer la table
    print("Création de la table si nécessaire...")
    create_table(connection)

    # Insérer les données
    print("Insertion des données...")
    insert_data(connection, df)

    # Valider les données
    print("\nValidation des données...")
    validate_data(connection)

    # Fermer la connexion
    connection.close()
    print("\nTraitement terminé.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='creation de la base de donné gold')
    parser.add_argument('--db_host', type=str, required=True, help='host sql')
    parser.add_argument('--db_user', type=str, required=True, help='Utilisateur MySQL')
    parser.add_argument('--db_password', type=str, required=True, help='Mot de passe MySQL')
    parser.add_argument('--db_silver_name', type=str, required=True, help='base de donnée silver')
    parser.add_argument('--bucket_name', type=str, required=True, help='bucket name')
    parser.add_argument('--localstack_url', type=str, required=True, help='url du localstack')

    args = parser.parse_args()

    loading_silver(args.db_host, args.db_user, args.db_password, args.localstack_url, args.bucket_name, args.db_silver_name)
