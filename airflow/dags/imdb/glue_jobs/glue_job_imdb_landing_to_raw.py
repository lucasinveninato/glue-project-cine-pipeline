import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import logging

# Inicialização do GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configurações de bucket e prefixo
source_bucket = 'project-cine'
target_bucket = 'project-cine'
source_prefix = 'landing-zone/'
target_prefix = 'raw-layer/'

# Cliente S3
s3_client = boto3.client('s3')

# Listar arquivos no prefixo de origem
response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

# Processar cada arquivo
for obj in response.get('Contents', []):
    key = obj['Key']
    
    if not key.endswith('/'):  # Ignorar diretórios
        # Construir caminhos de origem e destino
        source_path = f's3://{source_bucket}/{key}'
        file_name = key.split('/')[-1]
        target_key = key.replace(source_prefix, target_prefix, 1)
        target_path = f's3://{target_bucket}/{target_key}'

        # Ler o arquivo TSV
        df = spark.read.option("header", "true").option("delimiter", "\t").csv(source_path)

        # Gravar o DataFrame no S3 diretamente como Parquet
        df.write.parquet(f's3://{target_bucket}/{target_prefix}/{file_name.replace(".tsv", "")}', mode='overwrite', compression='snappy')

        logging.info(f"Converted data and uploaded from {source_path} to {target_path} successfully.")
