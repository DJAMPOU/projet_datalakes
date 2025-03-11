import argparse
import os
import boto3
import pandas as pd
from utils import download_and_unzip_kaggle_competition, upload_to_s3, create_bucket_if_not_exists

def loading_raw(S3_BUCKET_NAME, LOCALSTACK_URL, DATASET_NAME):

    output_dir = "data"  # Répertoire de destination pour le téléchargement

    # Télécharger et décompresser le dataset
    download_and_unzip_kaggle_competition(DATASET_NAME, output_dir)

    # Initialiser le client S3
    s3_client = boto3.client('s3',
                             endpoint_url=LOCALSTACK_URL,
                             aws_access_key_id="test",
                             aws_secret_access_key="test",
                             region_name="us-east-1")

    #creer le bucket si il n'existe pas
    create_bucket_if_not_exists(s3_client, S3_BUCKET_NAME)

    # Charger les fichiers train.csv et test.csv sur S3 après extraction
    train_file_path = os.path.join(output_dir, "train.csv")
    test_file_path = os.path.join(output_dir, "test.csv")

    # Vérifier l'existence des fichiers et les fusionner
    if os.path.exists(train_file_path) and os.path.exists(test_file_path):
        print("Fusion des fichiers train.csv et test.csv...")

        # Charger les fichiers dans des DataFrames pandas
        train_df = pd.read_csv(train_file_path)
        test_df = pd.read_csv(test_file_path)

        # Fusionner les deux fichiers
        combined_df = pd.concat([train_df, test_df], ignore_index=True)

        # Enregistrer le fichier fusionné
        combined_file_path = os.path.join(output_dir, "data.csv")
        combined_df.to_csv(combined_file_path, index=False)
        print(f"Fichier fusionné enregistré sous {combined_file_path}")

        # Charger le fichier fusionné dans S3 sous une seule clé 'data.csv'
        upload_to_s3(s3_client, combined_file_path, S3_BUCKET_NAME, "data.csv")

    else:
        print("Les fichiers train.csv et/ou test.csv sont introuvables. Fusion impossible.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='chargement de la raw')
    parser.add_argument('--bucket_name', type=str, required=True, help='bucket name')
    parser.add_argument('--localstack_url', type=str, required=True, help='url du localstack')
    parser.add_argument('--dataset_name', type=str, required=True, help='nom du data set')
    args = parser.parse_args()

    loading_raw(args.bucket_name, args.localstack_url, args.dataset_name)
