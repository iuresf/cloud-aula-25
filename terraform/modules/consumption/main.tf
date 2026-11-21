variable "project" { type = string }
variable "bucket" { type = string }
variable "database" { type = string }

resource "aws_athena_workgroup" "consumer" {
  name          = "${var.project}-consumer"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration {
      output_location = "s3://${var.bucket}/athena-consumer/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
resource "aws_athena_named_query" "vendas_mensais_v1" {
  name      = "vendas_mensais_v1"
  database  = var.database
  workgroup = aws_athena_workgroup.consumer.name
  query     = <<-SQL
    SELECT t.ano, t.mes, f.sk_regiao, f.sk_canal, CAST(SUM(f.receita_liquida) AS DECIMAL(14,2)) AS receita_liquida
    FROM fato_itens_venda f JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    GROUP BY t.ano, t.mes, f.sk_regiao, f.sk_canal
  SQL
}
output "workgroup" { value = aws_athena_workgroup.consumer.name }
