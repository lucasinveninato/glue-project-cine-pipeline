# glue-project-cine-pipeline
glue-project-cine-pipeline uses AWS Glue and Apache Airflow to build a data pipeline for IMDb data. It features a layered architecture: Landing (raw data), Raw (replicated data), Processed (cleaned and transformed data), Analytical (optimized for queries), and Consumption (data for reporting).


## Start up the project

Use this command in terminal to start airflow:

```
docker compose up airflow-init
```

