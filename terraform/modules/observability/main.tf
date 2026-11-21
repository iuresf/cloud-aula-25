variable "project" { type = string }
variable "region" { type = string }
variable "audit_bucket" { type = string }
variable "bronze_bucket" { type = string }
variable "function_names" { type = list(string) }
variable "dlq_names" { type = list(string) }
variable "state_machine_arn" { type = string }

resource "aws_cloudwatch_log_group" "ingestion" {
  for_each          = toset(var.function_names)
  name              = "/aws/lambda/${each.value}"
  retention_in_days = 30
}
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  for_each            = toset(var.dlq_names)
  alarm_name          = "${each.value}-messages"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  dimensions          = { QueueName = each.value }
  treat_missing_data  = "notBreaching"
}
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = toset(var.function_names)
  alarm_name          = "${each.value}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  dimensions          = { FunctionName = each.value }
  treat_missing_data  = "notBreaching"
}
resource "aws_cloudwatch_metric_alarm" "step_function_failures" {
  alarm_name          = "${var.project}-etl-executions-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  dimensions          = { StateMachineArn = var.state_machine_arn }
  treat_missing_data  = "notBreaching"
}
resource "aws_cloudwatch_dashboard" "lakehouse" {
  dashboard_name = "${var.project}-lakehouse"
  dashboard_body = jsonencode({ widgets = [
    {
      type = "metric", x = 0, y = 0, width = 12, height = 6,
      properties = { title = "Falhas de ingestão", view = "timeSeries", region = var.region, stat = "Sum",
      metrics = [for name in var.function_names : ["AWS/Lambda", "Errors", "FunctionName", name]] }
    },
    {
      type = "metric", x = 12, y = 0, width = 12, height = 6,
      properties = { title = "Execuções ETL com falha", view = "timeSeries", region = var.region, stat = "Sum",
      metrics = [["AWS/States", "ExecutionsFailed", "StateMachineArn", var.state_machine_arn]] }
    }
  ] })
}
resource "aws_s3_bucket_policy" "audit" {
  bucket = var.audit_bucket
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "AWSCloudTrailAclCheck", Effect = "Allow", Principal = { Service = "cloudtrail.amazonaws.com" }, Action = "s3:GetBucketAcl", Resource = "arn:aws:s3:::${var.audit_bucket}" },
      { Sid = "AWSCloudTrailWrite", Effect = "Allow", Principal = { Service = "cloudtrail.amazonaws.com" }, Action = "s3:PutObject", Resource = "arn:aws:s3:::${var.audit_bucket}/AWSLogs/*", Condition = { StringEquals = { "s3:x-amz-acl" = "bucket-owner-full-control" } } }
    ]
  })
}
resource "aws_cloudtrail" "lakehouse" {
  name                          = "${var.project}-audit"
  s3_bucket_name                = var.audit_bucket
  enable_log_file_validation    = true
  include_global_service_events = true
  event_selector {
    read_write_type           = "All"
    include_management_events = true
    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::${var.bronze_bucket}/"]
    }
  }
  depends_on = [aws_s3_bucket_policy.audit]
}
