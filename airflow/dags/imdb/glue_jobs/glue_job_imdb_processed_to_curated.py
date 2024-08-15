from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, row_number
from pyspark.sql.window import Window
import logging

try:
    # Inicialização do GlueContext
    sc = SparkContext().getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Configurações de bucket e prefixo
    bucket = 'project-cine'
    processed_prefix = 'processed-layer/'

    # Leitura dos arquivos Parquet
    df_name_basics = spark.read.parquet(f's3://{bucket}/{processed_prefix}/name_basics')
    df_principals = spark.read.parquet(f's3://{bucket}/{processed_prefix}/principals')
    df_title_basics = spark.read.parquet(f's3://{bucket}/{processed_prefix}/title_basics')

    # Explodir a coluna 'title_ids' para obter um DataFrame com uma linha para cada filme
    df_name_basics_expanded = df_name_basics.withColumn("title_id", explode(col("title_ids")))

    # Juntar com a tabela 'principals' para obter os filmes em que os atores participaram
    df_actor_movies = df_name_basics_expanded.join(df_principals, ["name_id", "title_id"], "inner")
    
    # Juntar com a tabela 'title_basics' para obter informações sobre os filmes
    df_actor_movies_with_genres = df_actor_movies.join(
        df_title_basics.alias("tb"), 
        df_actor_movies["title_id"] == col("tb.title_id"),
        "inner"
    ).drop("tb.title_id")  # Remove a coluna duplicada

    # Contar o número de filmes em que cada ator ou atriz participou
    df_actor_counts = df_actor_movies_with_genres.groupBy("name_id", "primary_name").agg(count("title_id").alias("movie_count"))

    # Encontrar os 1000 atores ou atrizes que mais participaram de filmes
    df_top_actors = df_actor_counts.orderBy(desc("movie_count")).limit(100)

    # Determinar o gênero de filme favorito para cada ator ou atriz
    df_genre_counts = df_actor_movies_with_genres.withColumn("genre", explode(col("genres")))\
                                                 .groupBy("name_id", "genre").agg(count("title_id").alias("genre_count"))

    # Selecionar o gênero favorito (aquele com maior contagem)
    window_spec = Window.partitionBy("name_id").orderBy(desc("genre_count"))
    df_genre_favorites = df_genre_counts.withColumn("rank", row_number().over(window_spec))\
                                        .filter(col("rank") == 1)\
                                        .drop("rank")

    # Juntar com o DataFrame de atores para obter as informações finais
    df_final = df_top_actors.join(df_genre_favorites, "name_id", "left")\
                            .join(df_name_basics.select("name_id", "primary_name"), "name_id", "left")

    # Adicionar coluna de ranking
    window_spec_rank = Window.orderBy(desc("movie_count"))
    df_final_with_rank = df_final.withColumn("ranking", row_number().over(window_spec_rank))

    # Salvar o resultado em um novo arquivo Parquet
    output_path = f's3://{bucket}/curated-layer/top_actors'
    df_final_with_rank.write.parquet(output_path, mode='overwrite', compression='snappy')

    logging.info(f"Arquivo final salvo em {output_path} com sucesso.")
    
except Exception as e:
    logging.error(f"Erro: {str(e)}")
    raise
