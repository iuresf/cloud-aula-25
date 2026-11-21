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
      }
      Gold = {
        Type       = "Task"
        Resource   = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = var.jobs["gold"], "Arguments.$" = "$.glue_arguments" }
        ResultPath = "$.gold_result"
        End        = true
      }
    }
  })
}
output "state_machine_arn" { value = aws_sfn_state_machine.etl.arn }
