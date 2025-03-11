# Utiliser une image officielle Apache Airflow
FROM apache/airflow:2.10.0

# Passer à l'utilisateur root pour effectuer les changements nécessaires
USER root

# Créer le répertoire .kaggle pour l'utilisateur airflow (si non existant)
RUN mkdir -p /home/airflow/.kaggle

# Copier le fichier kaggle.json dans ce répertoire
COPY ./kaggle.json /home/airflow/.kaggle/kaggle.json

# Assurer que l'utilisateur airflow existe déjà, et changer le propriétaire et les permissions
RUN chown airflow:root /home/airflow/.kaggle \
    && chmod 777 /home/airflow/.kaggle
RUN chown airflow:root /home/airflow/.kaggle/kaggle.json \
    && chmod 777 /home/airflow/.kaggle/kaggle.json

RUN export KAGGLE_CONFIG_DIR='/home/airflow/.kaggle/'
# Passer à l'utilisateur airflow pour les prochaines opérations
USER airflow

COPY ./requirements.txt /app/
WORKDIR /app
RUN pip install -r requirements.txt
