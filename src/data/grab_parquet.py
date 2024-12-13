import os
from minio import Minio, S3Error
import urllib.request
import pandas as pd
import sys
import requests

def main():
    grab_data()


    """Grab the data from New York Yellow Taxi

    This method downloads Parquet files of the New York Yellow Taxi. 
    Files are saved into the "../../data/raw" folder.
    """


def grab_data() -> None:
    """Télécharge les fichiers de données NYC Yellow Taxi Trip depuis NYC Open Data."""
    
    # URLs directes vers les fichiers Parquet sur NYC Open Data (exemples)
    file_urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-08.parquet"
    ]

    # Dossier de sauvegarde
    home_dir = os.path.expanduser("~")
    raw_data_dir = os.path.join(home_dir, "C:/Users/Babacar/Desktop/cours epsi/architecture decisionnelle/ATL-Datamart/data/raw")
    os.makedirs(raw_data_dir, exist_ok=True)

    for url in file_urls:
        file_name = url.split('/')[-1]
        file_path = os.path.join(raw_data_dir, file_name)
        print(f"Téléchargement de {file_name} depuis {url}...")

        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Fichier {file_name} téléchargé avec succès.")

            # Lecture et affichage des données avec pandas
            print(f"Lecture des données de {file_name}...")
            data = pd.read_parquet(file_path)
            print(data.head())  # Affiche les 5 premières lignes pour vérification

        except requests.exceptions.HTTPError as http_err:
            print(f"Erreur HTTP pour {file_name}: {http_err}")
        except Exception as e:
            print(f"Erreur lors du téléchargement de {file_name}: {e}")

   
def write_data_minio(bucket_name: str, folder_path: str):
    """
    This method puts all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "newyork-data-bucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    
     # Parcours des fichiers dans le dossier et upload sur MinIO
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        
        # Vérifier que le fichier a l'extension .parquet avant de l'uploader
        if os.path.isfile(file_path) and file_name.endswith(".parquet"):
            try:
                # Téléchargement du fichier vers MinIO
                client.fput_object(
                    bucket_name=bucket_name,
                    object_name=file_name,  # Nom de l'objet dans MinIO
                    file_path=file_path      # Chemin local du fichier
                )
                print(f"Fichier '{file_name}' téléchargé dans le bucket '{bucket_name}'.")
            except S3Error as e:
                print(f"Erreur lors du téléchargement de {file_name}: {e}")

# Variables pour le nom du bucket et le dossier local à uploader
bucket_name = "newyork-data-bucket"                          # Nom du bucket MinIO
folder_path = os.path.expanduser("~/Desktop/cours epsi/architecture decisionnelle/ATL-Datamart/data/raw")  # Dossier contenant les fichiers Parquet

# Exécution de la fonction
write_data_minio(bucket_name, folder_path)
    

if __name__ == '__main__':
    sys.exit(main())
