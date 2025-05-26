variable "database_name" {
  description = "Glue database name"
  type        = string
}

variable "datalake_bucket_name" {
  description = "The single S3 bucket for data lake storage"
  type        = string
}

variable "glue_role_arn" {
  type        = string
  description = "IAM Role ARN for Glue jobs"
}
