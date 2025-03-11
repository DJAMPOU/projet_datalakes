from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Permet aux tâches de s'exécuter même si la précédente a échoué
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG(
    dag_id='projet_data_lake_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données',
    schedule='@daily',
    #schedule_interval=timedelta(days=1),  # Correction ici
    catchup=False,  # Évite d'exécuter toutes les tâches en retard depuis start_date
)

# Tâche 1: Création de la base de données gold
create_db_task = BashOperator(
    task_id='create_gold_database',
    bash_command='python /opt/airflow/scripts/create_gold_database.py --db_host mysql --db_user root --db_password root --db_gold_name gold',
    dag=dag,
)

# Tâche 2: Extraction vers raw
raw_task = BashOperator(
    task_id='unpack_to_raw',
    bash_command='python /opt/airflow/scripts/load_raw.py --bucket_name raw --localstack_url http://localstack:4566 --dataset_name nyc-taxi-trip-duration',
    dag=dag,
)

# Tâche 3: Transformation vers silver
silver_task = BashOperator(
    task_id='preprocess_to_silver',
    bash_command='python /opt/airflow/scripts/load_silver.py --db_host mysql --db_user root --db_password root --db_silver_name silver --bucket_name raw --localstack_url http://localstack:4566',
    dag=dag,
)

# Tâche 4: Chargement vers gold
gold_task = BashOperator(
    task_id='process_to_gold',
    bash_command='python /opt/airflow/scripts/load_gold.py --db_host mysql --db_user root --db_password root --db_silver_name silver --db_gold_name gold',
    dag=dag,
)

# Définir l'ordre d'exécution des tâches
create_db_task >> raw_task >> silver_task >> gold_task