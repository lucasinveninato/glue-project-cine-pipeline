
resource "aws_iam_role" "project_cine_glue_role" {
  name = "project-cine-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_policy" "amazon_s3_full_access" {
  name        = "AmazonS3FullAccess"
  description = "Provides full access to Amazon S3"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "s3:*"
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "aws_glue_service_role" {
  name        = "AWSGlueServiceRole"
  description = "Provides AWS Glue service role permissions"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "glue:*",
          "s3:*",
          "logs:*",
          "athena:*",
          "quicksight:ListUserGroups",
          "quicksight:ListUsers",
          "quicksight:DescribeUser",
          "quicksight:GetDashboardEmbedUrl",
          "quicksight:GetSessionEmbedUrl",
          "quicksight:ListDashboards"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_full_access" {
  policy_arn = aws_iam_policy.amazon_s3_full_access.arn
  role     = aws_iam_role.project_cine_glue_role.name
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  policy_arn = aws_iam_policy.aws_glue_service_role.arn
  role     = aws_iam_role.project_cine_glue_role.name
}

resource "aws_iam_role" "project_cine_lambda_role" {
  name = "project-cine-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role     = aws_iam_role.project_cine_lambda_role.name
}

resource "aws_iam_role_policy_attachment" "lambda_s3_full_access" {
  policy_arn = aws_iam_policy.amazon_s3_full_access.arn
  role     = aws_iam_role.project_cine_lambda_role.name
}