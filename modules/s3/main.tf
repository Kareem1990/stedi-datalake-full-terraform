# Create a single S3 bucket for the data lake (landing + trusted + curated folders)
resource "aws_s3_bucket" "datalake" {
  bucket        = var.datalake_bucket_name
  force_destroy = true
  tags = {
    Name = var.datalake_bucket_name
  }
}

# Upload customer landing data to the datalake bucket
resource "aws_s3_object" "customer_files" {
  for_each = fileset("${path.module}/data/customer", "*.json")

  bucket       = aws_s3_bucket.datalake.bucket
  key          = "customer_landing/${each.value}"
  source       = "${path.module}/data/customer/${each.value}"
  etag         = filemd5("${path.module}/data/customer/${each.value}")
  content_type = "application/json"
}

# Upload accelerometer landing data to the datalake bucket
resource "aws_s3_object" "accelerometer_files" {
  for_each = fileset("${path.module}/data/accelerometer", "*.json")

  bucket       = aws_s3_bucket.datalake.bucket
  key          = "accelerometer_landing/${each.value}"
  source       = "${path.module}/data/accelerometer/${each.value}"
  etag         = filemd5("${path.module}/data/accelerometer/${each.value}")
  content_type = "application/json"
}

# Upload step trainer landing data to the datalake bucket
resource "aws_s3_object" "step_trainer_files" {
  for_each = fileset("${path.module}/data/step_trainer", "*.json")

  bucket       = aws_s3_bucket.datalake.bucket
  key          = "step_trainer_landing/${each.value}"
  source       = "${path.module}/data/step_trainer/${each.value}"
  etag         = filemd5("${path.module}/data/step_trainer/${each.value}")
  content_type = "application/json"
}

# Upload the Glue job script to the datalake bucket
resource "aws_s3_object" "glue_scripts" {
  for_each = fileset("${path.root}/scripts", "*.py")

  bucket       = aws_s3_bucket.datalake.bucket
  key          = "scripts/${each.value}"
  source       = "${path.root}/scripts/${each.value}"
  etag         = filemd5("${path.root}/scripts/${each.value}")
  content_type = "text/x-python"

  depends_on = [aws_s3_bucket.datalake]
}
