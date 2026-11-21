variable "role_arn" {
  type = string
}
variable "database" {
  type = string
}
variable "buckets" {
  type = map(string)
}
resource "aws_lakeformation_resource" "layer" {
  for_each = toset(["silver", "gold"])
  arn      = "arn:aws:s3:::${var.buckets[each.key]}"
  role_arn = var.role_arn
}
resource "aws_lakeformation_permissions" "database" {
  principal   = var.role_arn
  permissions = ["ALL"]
  database { name = var.database }
}
resource "aws_lakeformation_permissions" "table" {
  principal   = var.role_arn
  permissions = ["ALL"]
  table {
    database_name = var.database
    wildcard      = true
  }
}
resource "aws_lakeformation_lf_tag" "classification" {
  key    = "classification"
  values = ["internal", "restricted"]
}
resource "aws_lakeformation_lf_tag" "domain" {
  key    = "domain"
  values = ["commerce"]
}
output "lf_tags" {
  value = {
    classification = aws_lakeformation_lf_tag.classification.values
    domain         = aws_lakeformation_lf_tag.domain.values
  }
}
