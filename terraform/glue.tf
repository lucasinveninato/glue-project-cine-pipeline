resource "aws_glue_catalog_database" "project_cine_curated_database" {
  name = "project-cine-curated-database"
}

resource "aws_glue_catalog_database" "project_cine_processed_database" {
  name = "project-cine-processed-database"
}

resource "aws_glue_catalog_database" "project_cine_raw_database" {
  name = "project-cine-raw-database"
}

resource "aws_glue_crawler" "project_cine_curated_crawler" {
  name          = "project-cine-curated-crawler"
  role          = aws_iam_role.project_cine_glue_role.arn
  database_name = aws_glue_catalog_database.project_cine_curated_database.name
  s3_target {
    path = "s3://project-cine/curated-layer/"
  }
}

resource "aws_glue_crawler" "project_cine_raw_crawler" {
  name          = "project-cine-raw-crawler"
  role          = aws_iam_role.project_cine_glue_role.arn
  database_name = aws_glue_catalog_database.project_cine_raw_database.name
  s3_target {
    path = "s3://project-cine/raw-layer/"
  }
}

resource "aws_glue_crawler" "project_cine_processed_crawler" {
  name          = "project-cine-processed-crawler"
  role          = aws_iam_role.project_cine_glue_role.arn
  database_name = aws_glue_catalog_database.project_cine_processed_database.name
  s3_target {
    path = "s3://project-cine/processed-layer/"
  }
}

resource "aws_glue_trigger" "project_cine_curated_crawler_trigger" {
  name     = "project-cine-curated-crawler-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 8 1 * ? *)"
  actions {
    crawler_name = aws_glue_crawler.project_cine_curated_crawler.name
  }
}

resource "aws_glue_trigger" "project_cine_raw_crawler_trigger" {
  name     = "project-cine-raw-crawler-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 8 1 * ? *)"
  actions {
    crawler_name = aws_glue_crawler.project_cine_raw_crawler.name
  }
}

resource "aws_glue_trigger" "project_cine_processed_crawler_trigger" {
  name     = "project-cine-processed-crawler-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 8 1 * ? *)"
  actions {
    crawler_name = aws_glue_crawler.project_cine_processed_crawler.name
  }
}
