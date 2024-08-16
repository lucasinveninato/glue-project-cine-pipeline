from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, row_number, asc
from pyspark.sql.window import Window
import logging
from datetime import datetime


def read_parquet_file(spark, bucket, prefix, file_name):
    """
    Reads a Parquet file from an S3 bucket.

    Args:
        spark (SparkSession): Active Spark session.
        bucket (str): Name of the S3 bucket where the data is stored.
        prefix (str): Directory prefix in the S3 bucket.
        file_name (str): Name of the Parquet file to read.

    Returns:
        DataFrame: DataFrame read from the Parquet file.
    """
    file_path = f's3://{bucket}/{prefix}/{file_name}'
    return spark.read.parquet(file_path)


def process_actor_data(df_name_basics, df_principals, df_title_basics):
    """
    Processes actor data to determine the top actors by movie participation and their favorite genres.

    Args:
        df_name_basics (DataFrame): DataFrame containing basic name information.
        df_principals (DataFrame): DataFrame containing principal cast information.
        df_title_basics (DataFrame): DataFrame containing basic title information.

    Returns:
        DataFrame: Processed DataFrame containing top actors with their favorite genres, ordered by movie count and name_id.
    """
    # Explode the 'title_ids' column to get a DataFrame with one row per movie
    df_name_basics_expanded = df_name_basics.withColumn("title_id", explode(col("title_ids")))
    # Join with the 'principals' table to get the movies in which the actors participated
    df_actor_movies = df_name_basics_expanded.join(df_principals, ["name_id", "title_id"], "inner")
    # Define the current year and calculate the 10-year range
    current_year = datetime.now().year
    start_year_threshold = current_year - 10
    # Join with the 'title_basics' table to get movie information, filtering by the last 10 years
    df_actor_movies_with_genres = df_actor_movies.join(
        df_title_basics.withColumnRenamed("title_id", "tb_title_id"),
        df_actor_movies["title_id"] == col("tb_title_id"),
        "inner"
    ).filter(col("start_year").cast("int") >= start_year_threshold) \
     .drop("tb_title_id")
    # Count the number of movies each actor or actress participated in
    df_actor_counts = df_actor_movies_with_genres.groupBy("name_id", "primary_name") \
                                                .agg(count("title_id").alias("movie_count"))
    # Find the actors or actresses who participated in the most movies and order by movie_count and name_id
    df_top_actors = df_actor_counts.orderBy(desc("movie_count"), asc("name_id"))
    # Determine the favorite movie genre for each actor or actress
    df_genre_counts = df_actor_movies_with_genres.withColumn("genre", explode(col("genres")))\
                                                 .groupBy("name_id", "genre")\
                                                 .agg(count("title_id").alias("genre_count"))
    # Select the favorite genre (the one with the highest count)
    window_spec = Window.partitionBy("name_id").orderBy(desc("genre_count"))
    df_genre_favorites = df_genre_counts.withColumn("rank", row_number().over(window_spec))\
                                        .filter(col("rank") == 1)\
                                        .drop("rank")
    # Join with the actors DataFrame to get the final information, without duplicating the 'primary_name' column
    df_final = df_top_actors.join(df_genre_favorites, "name_id", "left")
    # Add ranking column
    window_spec_rank = Window.orderBy(desc("movie_count"), asc("name_id"))
    df_final_with_rank = df_final.withColumn("ranking", row_number().over(window_spec_rank))
    # Return the final DataFrame ordered by movie_count and name_id
    return df_final_with_rank


def write_parquet_file(df, bucket, prefix, output_name):
    """
    Writes a DataFrame to a Parquet file in an S3 bucket.

    Args:
        df (DataFrame): DataFrame to write to Parquet.
        bucket (str): Name of the S3 bucket where the data will be saved.
        prefix (str): Directory prefix in the S3 bucket where the data will be saved.
        output_name (str): Name of the Parquet file to create.

    Returns:
        None
    """
    output_path = f's3://{bucket}/{prefix}/{output_name}'
    df.write.parquet(output_path, mode='overwrite', compression='snappy')
    logging.info(f"Final file saved successfully at {output_path}.")


try:
    sc = SparkContext().getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    bucket = 'project-cine'
    processed_prefix = 'processed-layer/'
    curated_prefix = 'curated-layer/'
    
    df_name_basics = read_parquet_file(spark, bucket, processed_prefix, 'name_basics')
    df_principals = read_parquet_file(spark, bucket, processed_prefix, 'principals')
    df_title_basics = read_parquet_file(spark, bucket, processed_prefix, 'title_basics')

    df_final_ordered = process_actor_data(df_name_basics, df_principals, df_title_basics)

    write_parquet_file(df_final_ordered, bucket, curated_prefix, 'top_actors')

except Exception as e:
    logging.error(f"Error: {str(e)}")
    raise
