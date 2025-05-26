variable "role_name" {
  description = "Name of the IAM role for Glue"
  type        = string
}

variable "datalake_bucket_name" {
  description = "Unified S3 bucket for all zones"
  type        = string
}
