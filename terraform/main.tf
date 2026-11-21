terraform {
  required_version = ">= 1.5, < 2.0"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "= 5.44.0" }
    archive = { source = "hashicorp/archive", version = "= 2.4.2" }
  }
}
provider "aws" { region = var.region }
data "aws_caller_identity" "me" {}
locals { database = replace(var.project, "-", "_") }
module "storage" {
  source     = "./modules/storage"
  project    = var.project
  account_id = data.aws_caller_identity.me.account_id
  region     = var.region
}
module "catalog" {
  source   = "./modules/catalog"
  database = local.database
}
module "etl" {
  source            = "./modules/etl"
  project           = var.project
  role_arn          = var.lab_role_arn
  glue_version      = var.glue_version
  number_of_workers = var.number_of_workers
  database          = module.catalog.database
  buckets           = module.storage.buckets
}
module "orchestration" {
  source   = "./modules/orchestration"
  project  = var.project
  role_arn = var.lab_role_arn
  jobs     = module.etl.jobs
}
module "query" {
  source  = "./modules/query"
  project = var.project
  bucket  = module.storage.buckets["results"]
}
module "ingestion" {
  source                = "./modules/ingestion"
  project               = var.project
  account_id            = data.aws_caller_identity.me.account_id
  region                = var.region
  role_arn              = coalesce(var.ingestion_role_arn, var.lab_role_arn)
  bronze_bucket         = module.storage.buckets["bronze"]
  results_bucket        = module.storage.buckets["results"]
  quarantine_bucket     = module.storage.buckets["quarantine"]
  state_machine_arn     = module.orchestration.state_machine_arn
  cognito_domain_prefix = var.cognito_domain_prefix
}
module "observability" {
  source         = "./modules/observability"
  project        = var.project
  region         = var.region
  audit_bucket   = module.storage.buckets["audit"]
  bronze_bucket  = module.storage.buckets["bronze"]
  function_names = module.ingestion.function_names
  dlq_names      = module.ingestion.dlq_names
}
module "maintenance" {
  source   = "./modules/maintenance"
  project  = var.project
  role_arn = var.lab_role_arn
  database = module.catalog.database
  buckets  = module.storage.buckets
  enabled  = var.enable_iceberg_maintenance
}
module "consumption" {
  source   = "./modules/consumption"
  project  = var.project
  bucket   = module.storage.buckets["results"]
  database = module.catalog.database
}
module "governance" {
  count    = var.enable_lake_formation ? 1 : 0
  source   = "./modules/governance"
  role_arn = var.lab_role_arn
  database = module.catalog.database
  buckets  = module.storage.buckets
}
output "region" { value = var.region }
output "database" { value = module.catalog.database }
output "workgroup" { value = module.query.workgroup }
output "buckets" { value = module.storage.buckets }
output "state_machine_arn" { value = module.orchestration.state_machine_arn }
output "jobs" { value = module.etl.jobs }
output "ingestion_api_url" { value = module.ingestion.api_url }
output "cognito_token_url" { value = module.ingestion.token_url }
output "consumer_workgroup" { value = module.consumption.workgroup }
