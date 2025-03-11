import argparse
import pandas as pd
from utils import create_mysql_connection
from mysql.connector import Error
from geopy.distance import geodesic

def calculate_additional_insight(df):
    """
    Ajoute les colonnes distance_km, avg_speed_kmh et departure_day dans le dataframe.
    """
    # Calculer la distance en kilomètres entre le point de départ et d'arrivée
    def haversine(row):
        pickup = (row['pickup_latitude'], row['pickup_longitude'])
        dropoff = (row['dropoff_latitude'], row['dropoff_longitude'])
        return geodesic(pickup, dropoff).kilometers

    # Appliquer la formule de Haversine
    df['distance_km'] = df.apply(haversine, axis=1)

    # Calculer la vitesse moyenne (distance / durée)
    # La durée est convertie de secondes en heures
    df['avg_speed_kmh'] = df['distance_km'] / (df['trip_duration'] / 3600)

    # Colonne du jour de départ
    df['departure_day'] = pd.to_datetime(df['pickup_datetime']).dt.date

    return df



def create_gold_table(connection):
    """
    Crée la table gold_table si elle n'existe pas.
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_table (
                id VARCHAR(255) PRIMARY KEY,
                vendor_id INT NOT NULL,
                departure_day DATE NOT NULL,
                passenger_count INT NOT NULL,
                store_and_fwd_flag CHAR NOT NULL,
                trip_duration FLOAT NOT NULL,
                distance_km FLOAT NOT NULL,
                avg_speed_kmh FLOAT NOT NULL
            )
        """)
        connection.commit()
        print("Table gold_table créée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création de la table gold_table : {e}")

def insert_gold_data(connection, df, batch_size=2000):
    """
    Insère les données dans la table gold_table par batches.
    """
    try:
        cursor = connection.cursor()

        # Préparer la requête d'insertion
        insert_query = """
            INSERT INTO gold_table (id, vendor_id, departure_day, passenger_count, store_and_fwd_flag, 
                                    trip_duration, distance_km, avg_speed_kmh)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Diviser le DataFrame en batchs
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [(row['id'],
                       row['vendor_id'],
                       row['departure_day'],
                       row['passenger_count'],
                       row['store_and_fwd_flag'],
                       row['trip_duration'],
                       row['distance_km'],
                       row['avg_speed_kmh'])
                      for _, row in batch.iterrows()]

            cursor.executemany(insert_query, values)

            connection.commit()
            print(f"Inséré {len(values)} lignes du lot {i // batch_size + 1}")
        print("Insertion terminée.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des données dans gold_table : {e}")

def validate_data(connection):
    """Valide les données insérées avec des requêtes SQL."""
    try:
        cursor = connection.cursor()

        # Compte total des lignes
        cursor.execute("SELECT COUNT(*) FROM gold_table")
        total_count = cursor.fetchone()[0]
        print(f"Nombre total de lignes: {total_count}")

        # Compte des lignes non vides
        cursor.execute("SELECT COUNT(*) FROM gold_table WHERE store_and_fwd_flag='N'")
        store_and_fwd_flag_N_count = cursor.fetchone()[0]
        print(f"Nombre de lignes store_and_fwd_flag=='N': {store_and_fwd_flag_N_count}")

        # Exemple de quelques lignes
        cursor.execute("SELECT id,departure_day, distance_km, avg_speed_kmh FROM gold_table LIMIT 5")
        print("\nExemple de 5 premières lignes:")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, departure_day: {row[1]}, distance_km: {row[2]}, avg_speed_kmh :{row[3]}")

    except Error as e:
        print(f"Erreur lors de la validation des données: {e}")

def read_from_silver(DB_HOST, DB_USER, DB_PASSWORD, DB_SILVER_NAME):
    connection = create_mysql_connection(
        DB_HOST,
        DB_USER,
        DB_PASSWORD,
        DB_SILVER_NAME
    )
    if connection is None:
        return

    # Charger les données brutes depuis la table taxi_trip
    query = "SELECT * FROM taxi_trip"
    silver_df = pd.read_sql(query, connection)

    connection.close()
    return silver_df


# Pipeline principal
def loading_gold(DB_HOST, DB_USER, DB_PASSWORD,DB_NAME, DB_SILVER_NAME):

    silver_df = read_from_silver(DB_HOST, DB_USER, DB_PASSWORD, DB_SILVER_NAME)

    connection = create_mysql_connection(
        DB_HOST,
        DB_USER,
        DB_PASSWORD,
        DB_NAME
    )
    if connection is None:
        return

    """
    Pipeline pour enrichir les données raw et les charger dans la table gold.
    """
    # Étape 1: Calcul des colonnes distance_km, avg_speed_kmh et departure_date
    enriched_df = calculate_additional_insight(silver_df)

    # Étape 2: Créer la table gold si elle n'existe pas
    create_gold_table(connection)

    # Étape 3: Insérer les données enrichies dans la table gold
    insert_gold_data(connection, enriched_df)

    #Etapa 4 : Validation
    validate_data(connection)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='creation de la base de donné gold')
    parser.add_argument('--db_host', type=str, required=True, help='host sql')
    parser.add_argument('--db_user', type=str, required=True, help='Utilisateur MySQL')
    parser.add_argument('--db_password', type=str, required=True, help='Mot de passe MySQL')
    parser.add_argument('--db_silver_name', type=str, required=True, help='base de donnée silver')
    parser.add_argument('--db_gold_name', type=str, required=True, help='base de donnée gold')

    args = parser.parse_args()

    loading_gold(args.db_host, args.db_user, args.db_password, args.db_gold_name, args.db_silver_name)

