# glue-project-cine-pipeline
glue-project-cine-pipeline uses AWS Glue and Apache Airflow to build a data pipeline for IMDb data. It features a layered architecture: Landing (raw data), Raw (replicated data), Processed (cleaned and transformed data), Analytical (optimized for queries), and Consumption (data for reporting).

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

* Create aws credentials and region (example Extra: {us-east-1}) on Admin> Connections with name aws_default

### Runs
* The ingestion lambda runs once a month for a chronjob.
* Dag imdb executes once a month for a chronjob.
* Glue crawler executes once a month for a chronjob.
* It's possible to do queries on the 3 data layers, and connect the quicksights to databases.