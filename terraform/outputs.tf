output "s3_bucket_project_cine_glue" {
  value = aws_s3_bucket.project_cine_glue_bucket.bucket
}

output "s3_bucket_project_cine" {
  value = aws_s3_bucket.project_cine_bucket.bucket
}

output "glue_database_curated" {
  value = aws_glue_catalog_database.project_cine_curated_database.name
}

output "glue_database_processed" {
  value = aws_glue_catalog_database.project_cine_processed_database.name
}

output "glue_database_raw" {
  value = aws_glue_catalog_database.project_cine_raw_database.name
}

output "glue_crawler_curated" {
  value = aws_glue_crawler.project_cine_curated_crawler.name
}

output "glue_crawler_raw" {
  value = aws_glue_crawler.project_cine_raw_crawler.name
}

output "glue_crawler_processed" {
  value = aws_glue_crawler.project_cine_processed_crawler.name
}
