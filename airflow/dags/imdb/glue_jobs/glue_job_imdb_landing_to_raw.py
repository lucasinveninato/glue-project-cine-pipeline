import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import logging

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

source_bucket = 'project-cine'
target_bucket = 'project-cine'
source_prefix = 'landing-zone/'
target_prefix = 'raw-layer/'

s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

for obj in response.get('Contents', []):
    key = obj['Key']
    
    if not key.endswith('/'):
        source_path = f's3://{source_bucket}/{key}'
        file_name = key.split('/')[-1]
        target_key = key.replace(source_prefix, target_prefix, 1)
        target_path = f's3://{target_bucket}/{target_key}'

        df = spark.read.option("header", "true").option("delimiter", "\t").csv(source_path)
        df.write.parquet(f's3://{target_bucket}/{target_prefix}/{file_name.replace(".tsv", "")}', mode='overwrite', compression='snappy')

        logging.info(f"Converted data and uploaded from {source_path} to {target_path} successfully.")
