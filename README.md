# Description
This project is a data pipeline that ingests data from the public IMDb database via an AWS Lambda function, storing the data in the Landing Zone and processing it through the Raw, Processed, and Curated layers using AWS Glue jobs.

In the Raw layer, data from the Landing Zone is copied and transformed into Parquet format. The Processed layer involves cleaning and treating the data to make it clear and suitable for visualization. In the Curated layer, a Parquet file is created based on the processed data, incorporating several joins and rules to select and rank actors and actresses by the number of movies they have appeared in, as well as identifying their most frequent movie genre.

The main files used in the pipeline are:

**name_basics** : Information about actors and actresses, including the IDs of the movies they have appeared in.
**title_basics**: Information about movies.
**principals**: Information about movies and their cast.

## Start up the project

### Configure aws credentials:

* Use this guide to install the aws cli: 
***https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html***

* Run the in your terminal:

```
aws configure
```
* Set aws credentials, you need to have full access to S3, Glue and Lambda configured for your user to be able to create the services


### Setup AWS Infrastructure

* Use this guide to install the terraform: 
***https://developer.hashicorp.com/terraform/install***

Run these commands to setup the enviroment:

```
terraform init
terraform apply
```

### Init Airflow Web Server
* create .env file and copy this line to file:
```
AIRFLOW_UID=50000
```

* Use these commands in terminal to start airflow:

```
docker compose up airflow-init
docker compose up
```
* After environment up, connect to the web server on localhost:8080 and use user: airflow and pass: airflow
* Create aws credentials and region (example Extra: {us-east-1}) on Admin> Connections with name aws_default

## Runs
* The ingestion lambda runs once a month for a chronjob.
* Dag imdb executes once a month for a chronjob.
* Glue crawler executes once a month for a chronjob.
* It's possible to do queries on the 3 data layers, and connect the quicksights to databases.
