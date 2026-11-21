variable "project" { type = string }
variable "role_arn" { type = string }
variable "database" { type = string }
variable "buckets" { type = map(string) }
variable "enabled" { type = bool }

resource "aws_s3_object" "script" {
  bucket = var.buckets["results"]
  key    = "jobs/maintenance.py"
  source = "${path.module}/maintenance.py"
  etag   = filemd5("${path.module}/maintenance.py")
}
resource "aws_glue_job" "iceberg" {
  name              = "${var.project}-iceberg-maintenance"
  role_arn          = var.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.buckets["results"]}/${aws_s3_object.script.key}"
  }
  default_arguments = {
    "--job-language" = "python", "--datalake-formats" = "iceberg", "--enable-glue-datacatalog" = "true",
    "--DATABASE"     = var.database, "--GOLD" = var.buckets["gold"],
    "--conf"         = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${var.buckets["silver"]}/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }
}
resource "aws_cloudwatch_event_rule" "weekly" {
  name                = "${var.project}-iceberg-maintenance-weekly"
  schedule_expression = "cron(0 4 ? * SUN *)"
  state               = var.enabled ? "ENABLED" : "DISABLED"
}
resource "aws_iam_role" "scheduler" {
  count              = var.enabled ? 1 : 0
  name               = "${var.project}-maintenance-scheduler"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}
resource "aws_iam_role_policy" "scheduler" {
  count  = var.enabled ? 1 : 0
  role   = aws_iam_role.scheduler[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = "glue:StartJobRun", Resource = aws_glue_job.iceberg.arn }] })
}
resource "aws_cloudwatch_event_target" "weekly" {
  count    = var.enabled ? 1 : 0
  rule     = aws_cloudwatch_event_rule.weekly.name
  arn      = aws_glue_job.iceberg.arn
  role_arn = aws_iam_role.scheduler[0].arn
}
