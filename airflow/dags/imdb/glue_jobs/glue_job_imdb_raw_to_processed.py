from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split
import logging

def clean_df(df):
    columns = df.columns
    for column in columns:
        df = df.withColumn(column, when(col(column) == "\\N", None).otherwise(col(column)))
        df = df.withColumn(column, when(col(column) == "none", None).otherwise(col(column)))
    return df

try:
    sc = SparkContext().getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Leitura dos arquivos Parquet da camada Raw
    raw_bucket = 'project-cine'
    raw_prefix = 'raw-layer/'

    df_name_basics = spark.read.parquet(f's3://{raw_bucket}/{raw_prefix}/name_basics')
    df_title_basics = spark.read.parquet(f's3://{raw_bucket}/{raw_prefix}/title_basics')
    df_title_principals = spark.read.parquet(f's3://{raw_bucket}/{raw_prefix}/principals')
    
    df_name_basics = df_name_basics.withColumnRenamed('nconst', 'name_id')
    df_name_basics = df_name_basics.withColumnRenamed('primaryName', 'primary_name')
    df_name_basics = df_name_basics.withColumnRenamed('birthYear', 'birth_year')
    df_name_basics = df_name_basics.withColumnRenamed('deathYear', 'death_year')
    df_name_basics = df_name_basics.withColumnRenamed('primaryProfession', 'primary_profession')
    df_name_basics = df_name_basics.withColumnRenamed('knownForTitles', 'title_ids')
    
    df_title_basics = df_title_basics.withColumnRenamed('tconst', 'title_id')
    df_title_basics = df_title_basics.withColumnRenamed('titleType', 'title_type')
    df_title_basics = df_title_basics.withColumnRenamed('primaryTitle', 'primary_title')
    df_title_basics = df_title_basics.withColumnRenamed('originalTitle', 'original_title')
    df_title_basics = df_title_basics.withColumnRenamed('isAdult', 'is_adult')
    df_title_basics = df_title_basics.withColumnRenamed('startYear', 'start_year')
    df_title_basics = df_title_basics.withColumnRenamed('endYear', 'end_year')
    df_title_basics = df_title_basics.withColumnRenamed('runtimeMinutes', 'runtime_minutes')
    
    df_title_principals = df_title_principals.withColumnRenamed('tconst', 'title_id')
    df_title_principals = df_title_principals.withColumnRenamed('nconst', 'name_id')
 
    df_name_basics = clean_df(df_name_basics)
    df_title_basics = clean_df(df_title_basics)
    df_title_principals = clean_df(df_title_principals)

    df_name_basics = df_name_basics.withColumn('title_ids', split(col('title_ids'), ","))
    df_name_basics = df_name_basics.withColumn('primary_profession', split(col('primary_profession'), ","))
    

    df_title_basics = df_title_basics.withColumn('genres', split(col('genres'), ","))

    df_title_basics = df_title_basics.withColumn(
        "is_adult",
        when(col("is_adult") == "1", True).otherwise(False)
    )

    df_name_basics.cache()
    df_title_basics.cache()
    df_title_principals.cache()

    # Escrita na camada Processed
    processed_bucket = 'project-cine'
    processed_prefix = 'processed-layer/'

    df_name_basics.write.parquet(f's3://{processed_bucket}/{processed_prefix}/name_basics', mode='overwrite', compression='snappy')
    df_title_basics.write.parquet(f's3://{processed_bucket}/{processed_prefix}/title_basics', mode='overwrite', compression='snappy')
    df_title_principals.write.parquet(f's3://{processed_bucket}/{processed_prefix}/principals', mode='overwrite', compression='snappy')

except Exception as e:
    logging.error(f"Error: {str(e)}")
    raise
