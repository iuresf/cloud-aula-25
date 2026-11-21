variable "project" {
  type = string
}
variable "role_arn" {
  type = string
}
variable "glue_version" {
  type = string
}
variable "number_of_workers" {
  type = number
}
variable "database" {
  type = string
}
variable "buckets" {
  type = map(string)
}
locals {
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.warehouse=s3://${var.buckets["silver"]}/",
    "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  ])
}
resource "aws_s3_object" "script" {
  for_each = toset(["silver", "gold"])
  bucket   = var.buckets["results"]
  key      = "jobs/${each.key}.py"
  source   = "${path.module}/../../jobs/${each.key}.py"
  etag     = filemd5("${path.module}/../../jobs/${each.key}.py")
}
resource "aws_glue_job" "job" {
  for_each          = toset(["silver", "gold"])
  name              = "${var.project}-${each.key}"
  role_arn          = var.role_arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = var.number_of_workers
  timeout           = 20
  execution_property { max_concurrent_runs = 1 }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.buckets["results"]}/${aws_s3_object.script[each.key].key}"
  }
  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-glue-datacatalog"          = "true"
    "--conf"                             = local.iceberg_conf
    "--BRONZE"                           = var.buckets["bronze"]
    "--SILVER"                           = var.buckets["silver"]
    "--GOLD"                             = var.buckets["gold"]
    "--RESULTS"                          = var.buckets["results"]
    "--DATABASE"                         = var.database
    "--enable-continuous-cloudwatch-log" = "true"
  }
}
output "jobs" { value = { for k, v in aws_glue_job.job : k => v.name } }
