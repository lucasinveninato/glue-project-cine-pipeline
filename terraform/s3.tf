resource "aws_s3_bucket" "project_cine_glue_bucket" {
  bucket = "project-cine-glue-bucket"
}

resource "aws_s3_bucket" "project_cine_bucket" {
  bucket = "project-cine"
}

resource "aws_s3_object" "curated_layer" {
  bucket = aws_s3_bucket.project_cine_bucket.bucket
  key    = "curated-layer/"
  acl    = "private"
}

resource "aws_s3_object" "landing_zone" {
  bucket = aws_s3_bucket.project_cine_bucket.bucket
  key    = "landing-zone/"
  acl    = "private"
}

resource "aws_s3_object" "processed_layer" {
  bucket = aws_s3_bucket.project_cine_bucket.bucket
  key    = "processed-layer/"
  acl    = "private"
}

resource "aws_s3_object" "raw_layer" {
  bucket = aws_s3_bucket.project_cine_bucket.bucket
  key    = "raw-layer/"
  acl    = "private"
}
