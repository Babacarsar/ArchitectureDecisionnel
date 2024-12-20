from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import urllib.error

def download_parquet(**kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    filename = "yellow_tripdata_2024-01.parquet"
    folder_path = "/data/raw"  # Utiliser le chemin monté dans Docker
    download_path = os.path.join(folder_path, filename)

    # Assurez-vous que le répertoire existe
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    try:
        # Télécharger le fichier
        request.urlretrieve(url, download_path)
        print(f"Fichier téléchargé avec succès: {download_path}")
        return download_path  # Retourner le chemin pour la tâche suivante
    except urllib.error.URLError as e:
        raise RuntimeError(f"Échec du téléchargement du fichier parquet: {str(e)}") from e

def upload_file(**kwargs):
    client = Minio(
        "minio:9000",  # Assurez-vous que ce port est ouvert et accessible
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = 'rawnyc'
    filename = "yellow_tripdata_2024-01.parquet"
    folder_path = "/data/raw"  # Utiliser le chemin monté dans Docker
    download_path = os.path.join(folder_path, filename)

    try:
        # Vérifier si le bucket existe
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        # Télécharger le fichier dans MinIO
        client.fput_object(
            bucket_name=bucket,
            object_name=filename,
            file_path=download_path
        )
        print(f"Fichier téléversé avec succès dans MinIO: {filename}")
        os.remove(download_path)  # Supprimer le fichier après l'upload pour éviter la redondance
    except S3Error as e:
        raise RuntimeError(f"Erreur de téléchargement dans MinIO: {str(e)}") from e

# Définition du DAG
with DAG(
    dag_id='grab_nyc_data_to_minio',
    start_date=days_ago(1),
    schedule_interval=None,  # Définir une fréquence selon vos besoins
    catchup=False,
    tags=['minio', 'parquet', 'etl']
) as dag:
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )

    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file,
        op_kwargs={'download_path': "{{ task_instance.xcom_pull(task_ids='download_parquet') }}" }
    )

    # Définir l'ordre d'exécution
    t1 >> t2