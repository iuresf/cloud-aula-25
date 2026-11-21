variable "project" {
  type = string
}
variable "bucket" {
  type = string
}
resource "aws_athena_workgroup" "this" {
  name          = var.project
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration {
      output_location = "s3://${var.bucket}/athena/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
output "workgroup" { value = aws_athena_workgroup.this.name }
