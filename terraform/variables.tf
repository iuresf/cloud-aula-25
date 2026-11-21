variable "region" {
  type    = string
  default = "us-east-1"
}
variable "project" {
  type    = string
  default = "a25-iceberg"
}
variable "lab_role_arn" {
  type = string
}
variable "ingestion_role_arn" {
  description = "Role das Lambdas de ingestão. Se nula, reutiliza lab_role_arn."
  type        = string
  default     = null
  nullable    = true
}
variable "glue_version" {
  type    = string
  default = "4.0"
}
variable "number_of_workers" {
  type    = number
  default = 2
}
variable "enable_lake_formation" {
  type    = bool
  default = false
}
variable "cognito_domain_prefix" {
  description = "Prefixo global do domínio Cognito usado pelo fluxo client_credentials."
  type        = string
  default     = "a25-lakehouse-ingestion-795636653176"
}
variable "enable_iceberg_maintenance" {
  description = "Habilita a regra semanal de manutenção Iceberg; mantenha false até aprovar retenção."
  type        = bool
  default     = false
}
