import urllib.request
import os
import sys
import shutil
from minio import Minio
def main():
    grab_data_2023_to_2024()
    write_data_minio()

def grab_data_2023_to_2024() -> None:
    """Delete existing files and download files from January 2018 to August 2023 and save locally."""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    years = range(2023, 2024)  # From 2018 to 2023
    months = range(1, 3)
    data_dir = "C:/Users/Babacar/Desktop/cours epsi/architecture decisionnelle/ATL-Datamart/data/raw"

    # Supprimer les fichiers existants dans le dossier
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    for year in years:
        for month in months:
            if year == 2024 and month > 2:
                break
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            file_url = base_url + filename
            output_path = os.path.join(data_dir, filename)
            try:
                urllib.request.urlretrieve(file_url, output_path)
                print(f"Downloaded {filename}")
            except Exception as e:
                print(f"Failed to download {filename}: {e}")

def write_data_minio(bucket_name: str, folder_path: str):
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
folder_path = os.path.expanduser("C:/Users/pc/OneDrive/Documents/Architecture_decisionnel/ATL-Datamart-main/ATL-Datamart-main/data/raw")  # Dossier contenant les fichiers Parquet
# Exécution de la fonction
write_data_minio(bucket_name, folder_path)


if __name__ == '__main__':
    sys.exit(main())
