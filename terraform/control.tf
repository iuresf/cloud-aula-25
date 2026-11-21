resource "aws_dynamodb_table" "operations" {
  name         = "${var.project}-operations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  attribute {
    name = "run_id"
    type = "S"
  }
  point_in_time_recovery { enabled = true }
}

data "archive_file" "publisher" {
  type        = "zip"
  source_file = "${path.module}/control/publisher.py"
  output_path = "${path.module}/control/.build/publisher.zip"
}
resource "aws_lambda_function" "publisher" {
  function_name    = "${var.project}-publisher"
  role             = var.lab_role_arn
  handler          = "publisher.handler"
  runtime          = "python3.12"
  timeout          = 60
  filename         = data.archive_file.publisher.output_path
  source_code_hash = data.archive_file.publisher.output_base64sha256
  environment {
    variables = {
      RESULTS_BUCKET   = module.storage.buckets["results"]
      DATABASE         = module.catalog.database
      OPERATIONS_TABLE = aws_dynamodb_table.operations.name
    }
  }
}

data "archive_file" "events" {
  type        = "zip"
  source_file = "${path.module}/control/events.py"
  output_path = "${path.module}/control/.build/events.zip"
}
resource "aws_lambda_function" "events" {
  function_name    = "${var.project}-execution-control"
  role             = var.lab_role_arn
  handler          = "events.handler"
  runtime          = "python3.12"
  timeout          = 60
  filename         = data.archive_file.events.output_path
  source_code_hash = data.archive_file.events.output_base64sha256
  environment {
    variables = {
      OPERATIONS_TABLE  = aws_dynamodb_table.operations.name
      BATCH_TABLE       = module.ingestion.batch_table
      STATE_MACHINE_ARN = module.orchestration.state_machine_arn
    }
  }
}
resource "aws_cloudwatch_event_rule" "executions" {
  name = "${var.project}-executions"
  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [module.orchestration.state_machine_arn]
      status          = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
    }
  })
}
resource "aws_cloudwatch_event_rule" "reconcile" {
  name                = "${var.project}-reconcile"
  schedule_expression = "rate(1 hour)"
}
resource "aws_cloudwatch_event_target" "executions" {
  rule      = aws_cloudwatch_event_rule.executions.name
  target_id = "execution-control"
  arn       = aws_lambda_function.events.arn
}
resource "aws_cloudwatch_event_target" "reconcile" {
  rule      = aws_cloudwatch_event_rule.reconcile.name
  target_id = "execution-reconcile"
  arn       = aws_lambda_function.events.arn
}
resource "aws_lambda_permission" "events" {
  statement_id  = "AllowExecutionEvents"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.events.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.executions.arn
}
resource "aws_lambda_permission" "reconcile" {
  statement_id  = "AllowExecutionReconcile"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.events.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile.arn
}
output "operations_table" { value = aws_dynamodb_table.operations.name }
