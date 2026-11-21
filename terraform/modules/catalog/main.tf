variable "database" {
  type = string
}
resource "aws_glue_catalog_database" "this" { name = var.database }
output "database" { value = aws_glue_catalog_database.this.name }
