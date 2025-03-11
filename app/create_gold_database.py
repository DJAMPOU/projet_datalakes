import argparse
import mysql.connector

def create_gold_database(DB_HOST, DB_USER, DB_PASSWORD, DB_GOLD_NAME):

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='creation de la base de donné gold')
    parser.add_argument('--db_host', type=str, required=True, help='host sql')
    parser.add_argument('--db_user', type=str, required=True, help='Utilisateur MySQL')
    parser.add_argument('--db_password', type=str, required=True, help='Mot de passe MySQL')
    parser.add_argument('--db_gold_name', type=str, required=True, help='base de données gold')

    args = parser.parse_args()

    create_gold_database(args.db_host, args.db_user, args.db_password, args.db_gold_name)
