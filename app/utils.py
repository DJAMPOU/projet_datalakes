import os
import subprocess
import zipfile
from mysql.connector import Error
from botocore.exceptions import ClientError
import mysql.connector
from kaggle.api.kaggle_api_extended import KaggleApi

def download_and_unzip_kaggle_competition(dataset, output_path):
    """Télécharge un dataset depuis une compétition Kaggle et le décompresse."""

    # Vérification du fichier kaggle.json
    kaggle_json_path = os.path.expanduser("~/.kaggle/kaggle.json")

    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError("Le fichier kaggle.json est introuvable. Veuillez placer votre fichier kaggle.json dans le dossier ~/.kaggle/")

    # Assurer que le dossier .kaggle est présent pour l'authentification
    os.environ["KAGGLE_CONFIG_DIR"] = os.path.dirname(kaggle_json_path)

    # Créer le répertoire de destination si il n'existe pas
    os.makedirs(output_path, exist_ok=True)

    api = KaggleApi()
    api.authenticate()
    print("ok5")

    try:
        # Exécution de la commande
        api.competition_download_files(dataset, path=output_path, force=True)
        print(f"Dataset téléchargé dans {output_path}")

        # Vérifier si le fichier téléchargé est un zip et le décompresser
        zip_file = os.path.join(output_path, f"{dataset}.zip")
        if os.path.exists(zip_file):
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(output_path)
            print(f"Fichiers extraits dans {output_path}")
            # Supprimer le fichier zip après extraction
            os.remove(zip_file)
        else:
            print("Le fichier n'est pas au format zip ou il n'a pas été trouvé.")

        # Décompresser train.zip et test.zip si présents
        for zip_filename in ['train.zip', 'test.zip']:
            zip_path = os.path.join(output_path, zip_filename)
            if os.path.exists(zip_path):
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(output_path)
                print(f"Fichiers {zip_filename} extraits dans {output_path}")
                # Supprimer les fichiers zip après extraction
                os.remove(zip_path)

    except subprocess.CalledProcessError as e:
        raise Exception(f"Erreur lors du téléchargement du dataset : {e}")


def upload_to_s3(s3_client, file_path, bucket_name, s3_key):
    """Charge un fichier dans un bucket S3."""
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Fichier {file_path} chargé dans {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Erreur lors du chargement sur S3 : {e}")



def create_bucket_if_not_exists(s3_client, bucket_name):
    """Crée un bucket S3 s'il n'existe pas déjà."""
    try:
        response = s3_client.list_buckets()
        existing_buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]

        if bucket_name not in existing_buckets:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' créé avec succès.")
        else:
            print(f"Bucket '{bucket_name}' existe déjà.")
    except ClientError as e:
        print(f"Erreur lors de la vérification ou de la création du bucket : {e}")

def create_mysql_connection(host, user, password, database):
    """Crée une connexion MySQL."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except Error as e:
        print(f"Erreur lors de la connexion à MySQL: {e}")
        return None