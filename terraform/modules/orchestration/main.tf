variable "project" {
  type = string
}
variable "role_arn" {
  type = string
}
variable "jobs" {
  type = map(string)
}
resource "aws_sfn_state_machine" "etl" {
  name     = "${var.project}-etl"
  role_arn = var.role_arn
  definition = jsonencode({
    Comment = "Silver e Gold: commits Iceberg independentes por tabela"
    StartAt = "Silver"
    States = {
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
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FalhaSilver"
        }]
      }
      Gold = {
        Type       = "Task"
        Resource   = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.jobs["gold"], "Arguments.$" = "$.glue_arguments" }
        ResultPath = "$.gold_result"
        End        = true
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException", "Glue.InternalServiceException", "Glue.OperationTimeoutException", "States.Timeout"]
          IntervalSeconds = 15
          BackoffRate     = 2.0
          MaxAttempts     = 3
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FalhaGold"
        }]
      }
      FalhaSilver = {
        Type  = "Fail"
        Error = "SilverJobFailed"
        Cause = "O job Silver falhou; Gold nao foi iniciado. Detalhes em $.error e no Glue."
      }
      FalhaGold = {
        Type  = "Fail"
        Error = "GoldJobFailed"
        Cause = "O job Gold falhou; o commit Silver anterior permanece rastreavel no Iceberg."
      }
    }
  })
}
output "state_machine_arn" { value = aws_sfn_state_machine.etl.arn }
