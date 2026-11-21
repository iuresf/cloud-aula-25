variable "project" {
  type = string
}
variable "account_id" {
  type = string
}
variable "region" {
  type = string
}
locals {
  buckets = { for z in ["bronze", "silver", "gold", "results", "quarantine", "audit"] : z => "${var.project}-${z}-${var.account_id}-${var.region}" }
}
resource "aws_s3_bucket_lifecycle_configuration" "layers" {
  for_each = toset(["bronze", "silver", "gold", "results", "quarantine", "audit"])
  bucket   = terraform_data.bucket[each.key].input.name
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}
resource "terraform_data" "bucket" {
  for_each = local.buckets
  input    = { name = each.value, region = var.region, script = "${path.module}/../../scripts/buckets.sh" }
  provisioner "local-exec" { command = "bash ${self.input.script} create ${self.input.name} ${self.input.region}" }
  provisioner "local-exec" {
    when    = destroy
    command = "bash ${self.input.script} destroy ${self.input.name} ${self.input.region}"
  }
}
output "buckets" {
  value      = local.buckets
  depends_on = [terraform_data.bucket]
}
