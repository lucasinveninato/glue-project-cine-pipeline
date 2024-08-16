# lambda.tf

resource "aws_lambda_function" "process_imdb_data" {
  filename         = "ingest_imdb_files.zip"
  function_name    = "process_imdb_data"
  role             = aws_iam_role.project_cine_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  memory_size      = 2500
  timeout          = 900

  environment {
    variables = {
      BUCKET_NAME            = "project-cine"
      LANDING_ZONE_PREFIX    = "landing-zone/"
    }
  }
    ephemeral_storage {
        size = 7000
    }
  
  source_code_hash = filebase64sha256("ingest_imdb_files.zip")
}

resource "aws_cloudwatch_event_rule" "monthly_trigger" {
  name                = "invoke_lambda_monthly"
  schedule_expression = "cron(0 6 1 * ? *)"
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke" {
  statement_id  = "AllowCloudWatchInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.process_imdb_data.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.monthly_trigger.arn
}

resource "aws_cloudwatch_event_target" "invoke_lambda_target" {
  rule      = aws_cloudwatch_event_rule.monthly_trigger.name
  target_id = "lambda"
  arn       = aws_lambda_function.process_imdb_data.arn
}
