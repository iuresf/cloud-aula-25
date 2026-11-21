variable "project" { type = string }
variable "account_id" { type = string }
variable "region" { type = string }
variable "role_arn" {
  type = string
}
variable "bronze_bucket" { type = string }
variable "results_bucket" { type = string }
variable "quarantine_bucket" { type = string }
variable "state_machine_arn" { type = string }
variable "cognito_domain_prefix" { type = string }

locals {
  functions = {
    api          = "api_ingestion.py"
    receipt      = "receipt.py"
    orchestrator = "orchestrator.py"
  }
  lambda_role_arn = var.role_arn
}

data "archive_file" "lambda" {
  for_each    = local.functions
  type        = "zip"
  source_file = "${path.module}/lambdas/${each.value}"
  output_path = "${path.module}/.build/${each.key}.zip"
}

resource "aws_dynamodb_table" "ingestion" {
  name         = "${var.project}-ingestion-ledger"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "object_version"
  attribute {
    name = "object_version"
    type = "S"
  }
  point_in_time_recovery { enabled = true }
}

resource "aws_cognito_user_pool" "ingestors" {
  name                     = "${var.project}-ingestors"
  auto_verified_attributes = []
  username_configuration { case_sensitive = false }
}
resource "aws_cognito_resource_server" "ingestion" {
  identifier   = "ingestion"
  name         = "Ingestion API"
  user_pool_id = aws_cognito_user_pool.ingestors.id
  scope {
    scope_name        = "write"
    scope_description = "Criar uma sessão de ingestão"
  }
}
resource "aws_cognito_user_pool_domain" "ingestors" {
  domain       = var.cognito_domain_prefix
  user_pool_id = aws_cognito_user_pool.ingestors.id
}
resource "aws_cognito_user_pool_client" "machine" {
  name                                 = "${var.project}-machine-ingestion"
  user_pool_id                         = aws_cognito_user_pool.ingestors.id
  generate_secret                      = true
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = ["ingestion/write"]
  supported_identity_providers         = ["COGNITO"]
  depends_on                           = [aws_cognito_resource_server.ingestion]
}

resource "aws_sqs_queue" "object_dlq" {
  name                      = "${var.project}-object-events-dlq"
  message_retention_seconds = 1209600
}
resource "aws_sqs_queue" "object_events" {
  name                       = "${var.project}-object-events"
  visibility_timeout_seconds = 180
  redrive_policy             = jsonencode({ deadLetterTargetArn = aws_sqs_queue.object_dlq.arn, maxReceiveCount = 5 })
}
resource "aws_sqs_queue" "ready_dlq" {
  name                      = "${var.project}-ready-dlq"
  message_retention_seconds = 1209600
}
resource "aws_sqs_queue" "ready" {
  name                       = "${var.project}-ready"
  visibility_timeout_seconds = 180
  redrive_policy             = jsonencode({ deadLetterTargetArn = aws_sqs_queue.ready_dlq.arn, maxReceiveCount = 5 })
}
resource "aws_sqs_queue_policy" "object_events" {
  queue_url = aws_sqs_queue.object_events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow", Principal = { Service = "s3.amazonaws.com" }, Action = "sqs:SendMessage",
      Resource  = aws_sqs_queue.object_events.arn,
      Condition = { ArnEquals = { "aws:SourceArn" = "arn:aws:s3:::${var.bronze_bucket}" } }
    }]
  })
}

resource "aws_lambda_function" "api" {
  function_name    = "${var.project}-api-ingestion"
  role             = local.lambda_role_arn
  handler          = "api_ingestion.handler"
  runtime          = "python3.12"
  timeout          = 20
  filename         = data.archive_file.lambda["api"].output_path
  source_code_hash = data.archive_file.lambda["api"].output_base64sha256
  environment { variables = { BRONZE_BUCKET = var.bronze_bucket, MAX_UPLOAD_BYTES = "104857600" } }
}
resource "aws_lambda_function" "receipt" {
  function_name    = "${var.project}-receipt"
  role             = local.lambda_role_arn
  handler          = "receipt.handler"
  runtime          = "python3.12"
  timeout          = 25
  filename         = data.archive_file.lambda["receipt"].output_path
  source_code_hash = data.archive_file.lambda["receipt"].output_base64sha256
  environment { variables = {
    BRONZE_BUCKET     = var.bronze_bucket, RESULTS_BUCKET = var.results_bucket,
    QUARANTINE_BUCKET = var.quarantine_bucket, READY_QUEUE_URL = aws_sqs_queue.ready.id,
    LEDGER_TABLE      = aws_dynamodb_table.ingestion.name
  } }
}
resource "aws_lambda_function" "orchestrator" {
  function_name    = "${var.project}-orchestrator"
  role             = local.lambda_role_arn
  handler          = "orchestrator.handler"
  runtime          = "python3.12"
  timeout          = 20
  filename         = data.archive_file.lambda["orchestrator"].output_path
  source_code_hash = data.archive_file.lambda["orchestrator"].output_base64sha256
  environment { variables = { STATE_MACHINE_ARN = var.state_machine_arn, LEDGER_TABLE = aws_dynamodb_table.ingestion.name } }
}
resource "aws_lambda_event_source_mapping" "receipt" {
  event_source_arn        = aws_sqs_queue.object_events.arn
  function_name           = aws_lambda_function.receipt.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}
resource "aws_lambda_event_source_mapping" "orchestrator" {
  event_source_arn        = aws_sqs_queue.ready.arn
  function_name           = aws_lambda_function.orchestrator.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_api_gateway_rest_api" "ingestion" {
  name = "${var.project}-ingestion"
}
resource "aws_api_gateway_resource" "ingestions" {
  rest_api_id = aws_api_gateway_rest_api.ingestion.id
  parent_id   = aws_api_gateway_rest_api.ingestion.root_resource_id
  path_part   = "ingestions"
}
resource "aws_api_gateway_authorizer" "cognito" {
  name            = "cognito-ingestors"
  rest_api_id     = aws_api_gateway_rest_api.ingestion.id
  type            = "COGNITO_USER_POOLS"
  provider_arns   = [aws_cognito_user_pool.ingestors.arn]
  identity_source = "method.request.header.Authorization"
}
resource "aws_api_gateway_method" "ingestion" {
  rest_api_id          = aws_api_gateway_rest_api.ingestion.id
  resource_id          = aws_api_gateway_resource.ingestions.id
  http_method          = "POST"
  authorization        = "COGNITO_USER_POOLS"
  authorizer_id        = aws_api_gateway_authorizer.cognito.id
  authorization_scopes = ["ingestion/write"]
}
resource "aws_api_gateway_integration" "ingestion" {
  rest_api_id             = aws_api_gateway_rest_api.ingestion.id
  resource_id             = aws_api_gateway_resource.ingestions.id
  http_method             = aws_api_gateway_method.ingestion.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api.invoke_arn
}
resource "aws_api_gateway_deployment" "ingestion" {
  rest_api_id = aws_api_gateway_rest_api.ingestion.id
  triggers = {
    redeployment = sha1(jsonencode([aws_api_gateway_method.ingestion.id, aws_api_gateway_integration.ingestion.id]))
  }
  lifecycle { create_before_destroy = true }
}
resource "aws_api_gateway_stage" "ingestion" {
  rest_api_id           = aws_api_gateway_rest_api.ingestion.id
  deployment_id         = aws_api_gateway_deployment.ingestion.id
  stage_name            = "v1"
  cache_cluster_enabled = false
  xray_tracing_enabled  = true
}
resource "aws_api_gateway_method_settings" "ingestion" {
  rest_api_id = aws_api_gateway_rest_api.ingestion.id
  stage_name  = aws_api_gateway_stage.ingestion.stage_name
  method_path = "ingestions/POST"
  settings {
    throttling_burst_limit = 50
    throttling_rate_limit  = 25
    metrics_enabled        = true
  }
}
resource "aws_wafv2_web_acl" "ingestion" {
  name  = "${var.project}-ingestion"
  scope = "REGIONAL"
  default_action {
    allow {}
  }
  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.project}-ingestion-waf"
    sampled_requests_enabled   = true
  }
  rule {
    name     = "rate-limit"
    priority = 1
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = 1000
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "rate-limit"
      sampled_requests_enabled   = true
    }
  }
  rule {
    name     = "aws-common-rules"
    priority = 2
    override_action {
      none {}
    }
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "aws-common-rules"
      sampled_requests_enabled   = true
    }
  }
}
resource "aws_wafv2_web_acl_association" "ingestion" {
  resource_arn = aws_api_gateway_stage.ingestion.arn
  web_acl_arn  = aws_wafv2_web_acl.ingestion.arn
}
resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.ingestion.execution_arn}/*/*"
}
resource "aws_s3_bucket_notification" "bronze" {
  bucket = var.bronze_bucket
  queue {
    queue_arn     = aws_sqs_queue.object_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "ingestion/"
  }
  depends_on = [aws_sqs_queue_policy.object_events]
}

output "api_url" { value = "https://${aws_api_gateway_rest_api.ingestion.id}.execute-api.${var.region}.amazonaws.com/${aws_api_gateway_stage.ingestion.stage_name}" }
output "token_url" { value = "https://${aws_cognito_user_pool_domain.ingestors.domain}.auth.${var.region}.amazoncognito.com/oauth2/token" }
output "function_names" { value = [aws_lambda_function.api.function_name, aws_lambda_function.receipt.function_name, aws_lambda_function.orchestrator.function_name] }
output "dlq_names" { value = [aws_sqs_queue.object_dlq.name, aws_sqs_queue.ready_dlq.name] }
