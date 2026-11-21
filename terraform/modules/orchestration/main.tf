variable "project" {
  type = string
}
variable "role_arn" {
  type = string
}
variable "jobs" {
  type = map(string)
}
variable "publisher_arn" { type = string }
resource "aws_sfn_state_machine" "etl" {
  name     = "${var.project}-etl"
  role_arn = var.role_arn
  definition = jsonencode({
    Comment = "Silver e Gold: commits Iceberg independentes por tabela"
    StartAt = "Route"

    States = {
      Route = {
        Type    = "Choice"
        Choices = [{ Variable = "$.start_from", StringEquals = "GOLD", Next = "Gold" }]
        Default = "Silver"
      }
      Silver = {
        Type       = "Task"
        Resource   = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.jobs["silver"], "Arguments.$" = "$.glue_arguments" }
        ResultPath = "$.silver_result"
        Next       = "Gold"
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException", "Glue.InternalServiceException", "Glue.OperationTimeoutException", "States.Timeout"]
          IntervalSeconds = 15
          BackoffRate     = 2.0
          MaxAttempts     = 3
        }]

      }
      Gold = {
        Type       = "Task"
        Resource   = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.jobs["gold"], "Arguments.$" = "$.glue_arguments" }
        ResultPath = "$.gold_result"
        Next       = "Publish"
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException", "Glue.InternalServiceException", "Glue.OperationTimeoutException", "States.Timeout"]
          IntervalSeconds = 15
          BackoffRate     = 2.0
          MaxAttempts     = 3
        }]

      }
      Publish = {
        Type       = "Task"
        Resource   = "arn:aws:states:::lambda:invoke"
        Parameters = { FunctionName = var.publisher_arn, "Payload.$" = "$" }
        End        = true
      }
    }
  })
}
output "state_machine_arn" { value = aws_sfn_state_machine.etl.arn }
