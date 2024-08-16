import boto3
import gzip
import os
import urllib.request

s3 = boto3.client('s3')

file_urls = {
    "name.basics.tsv.gz": "https://datasets.imdbws.com/name.basics.tsv.gz",
    "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.principals.tsv.gz": "https://datasets.imdbws.com/title.principals.tsv.gz"
}

renamed_files = {
    "name.basics.tsv.gz": "name_basics.tsv",
    "title.basics.tsv.gz": "title_basics.tsv",
    "title.principals.tsv.gz": "principals.tsv"
}

bucket_name = "project-cine"
landing_zone_prefix = "landing-zone/"

def download_and_extract(url, file_name):
    local_gz_path = f"/tmp/{file_name}"
    local_tsv_path = f"/tmp/{renamed_files[file_name]}"
    urllib.request.urlretrieve(url, local_gz_path)
    with gzip.open(local_gz_path, 'rb') as f_in, open(local_tsv_path, 'wb') as f_out:
        while True:
            chunk = f_in.read(1024)
            if not chunk:
                break
            f_out.write(chunk)

    return local_tsv_path

def upload_to_s3(file_name, bucket, s3_key):
    try:
        s3.upload_file(file_name, bucket, s3_key)
        print(f"Uploaded {file_name} to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_name} to s3://{bucket}/{s3_key}: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        for file_name, url in file_urls.items():
            local_file_path = download_and_extract(url, file_name)
            upload_to_s3(local_file_path, bucket_name, f"{landing_zone_prefix}{renamed_files[file_name]}")
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

    except Exception as e:
        print(f"Erro ao processar arquivos: {str(e)}")
        raise e
