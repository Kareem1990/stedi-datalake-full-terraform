resource "aws_glue_catalog_database" "stedi" {
  name = var.database_name
}

resource "aws_glue_catalog_table" "customer_landing" {
  name          = "customer_landing"
  database_name = aws_glue_catalog_database.stedi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification     = "json"
    has_encrypted_data = "false"
  }

  storage_descriptor {
    location      = "s3://${var.datalake_bucket_name}/customer_landing/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "email"
      type = "string"
    }
    columns {
      name = "serialNumber"
      type = "string"
    }
    columns {
      name = "birthDay"
      type = "string"
    }
    columns {
      name = "registrationDate"
      type = "string"
    }
    columns {
      name = "shareWithResearchAsOfDate"
      type = "bigint"
    }
  }
}
#Customer Landing to Trusted
resource "aws_glue_job" "customer_trusted" {
  name     = "customer_landing_to_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/customer_landing_to_trusted.py"
  }

  glue_version = "4.0"
  max_retries  = 0
  timeout      = 10

  default_arguments = {
    "--TempDir"                      = "s3://${var.datalake_bucket_name}/temp/"
    "--job-language"                 = "python"
    "--enable-glue-datacatalog"     = "true"
  }

  depends_on = [aws_glue_catalog_table.customer_landing]
}

# ACCELEROMETER Landing to Trusted
resource "aws_glue_job" "accelerometer_trusted" {
  name     = "accelerometer_landing_to_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/accelerometer_landing_to_trusted.py"
  }

  glue_version      = "4.0"
  timeout           = 10
  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }
}

# STEP TRAINER Landing to Trusted
resource "aws_glue_job" "step_trainer_trusted" {
  name     = "step_trainer_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/step_trainer_trusted.py"
  }

  glue_version      = "4.0"
  timeout           = 10
  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }
}

# Final Join - Curated Zone
resource "aws_glue_job" "machine_learning_curated" {
  name     = "machine_learning_curated"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/machine_learning_curated.py"
  }

  glue_version      = "4.0"
  timeout           = 10
  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }
}
