resource "aws_glue_catalog_database" "stedi" {
  name = var.database_name
}

# -------------------------------
# CUSTOMER LANDING TABLE
# -------------------------------
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

# CUSTOMER TRUSTED JOB
resource "aws_glue_job" "customer_trusted" {
  name     = "customer_landing_to_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/customer_landing_to_trusted.py"
  }

  glue_version = "4.0"
  timeout      = 10
  max_retries  = 0

  default_arguments = {
    "--TempDir"                  = "s3://${var.datalake_bucket_name}/temp/"
    "--job-language"             = "python"
    "--enable-glue-datacatalog" = "true"
  }

  depends_on = [aws_glue_catalog_table.customer_landing]
}

# -------------------------------
# ACCELEROMETER LANDING TABLE
# -------------------------------
resource "aws_glue_catalog_table" "accelerometer_landing" {
  name          = "accelerometer_landing"
  database_name = aws_glue_catalog_database.stedi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification     = "json"
    has_encrypted_data = "false"
  }

  storage_descriptor {
    location      = "s3://${var.datalake_bucket_name}/accelerometer_landing/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "user"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "bigint"
    }
    columns {
      name = "x"
      type = "double"
    }
    columns {
      name = "y"
      type = "double"
    }
    columns {
      name = "z"
      type = "double"
    }
  }
}

# ACCELEROMETER TRUSTED JOB
resource "aws_glue_job" "accelerometer_trusted" {
  name     = "accelerometer_landing_to_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/accelerometer_landing_to_trusted.py"
  }

  glue_version = "4.0"
  timeout      = 10
  max_retries  = 0

  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }

  depends_on = [
    aws_glue_catalog_table.accelerometer_landing
  ]
}

resource "aws_glue_job" "customer_trusted_to_curated" {
  name     = "customer_trusted_to_curated"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://stedi-datalake-terraform-kr/scripts/customer_trusted_to_curated.py"
    python_version  = "3"
  }

  glue_version = "4.0"

  default_arguments = {
    "--job-language"                        = "python"
    "--enable-metrics"                      = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--enable-glue-datacatalog"             = "true"
    "--TempDir"                             = "s3://stedi-datalake-terraform-kr/tmp/"
  }

  max_retries       = 1
  execution_class   = "STANDARD"
  number_of_workers = 2
  worker_type       = "G.1X"
}


# -------------------------------
# CUSTOMER CURATED TABLE
# -------------------------------
resource "aws_glue_catalog_table" "customer_curated" {
  name          = "customer_curated"
  database_name = aws_glue_catalog_database.stedi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification     = "parquet"
    has_encrypted_data = "false"
  }

  storage_descriptor {
    location      = "s3://${var.datalake_bucket_name}/customer_curated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "customername"
      type = "string"
    }
    columns {
      name = "email"
      type = "string"
    }
    columns {
      name = "phone"
      type = "string"
    }
    columns {
      name = "birthday"
      type = "string"
    }
    columns {
      name = "serialnumber"
      type = "string"
    }
    columns {
      name = "registrationdate"
      type = "bigint"
    }
    columns {
      name = "lastupdatedate"
      type = "bigint"
    }
    columns {
      name = "sharewithresearchasofdate"
      type = "bigint"
    }
    columns {
      name = "sharewithpublicasofdate"
      type = "bigint"
    }
    columns {
      name = "sharewithfriendsasofdate"
      type = "bigint"
    }
  }
}


# -------------------------------
# STEP TRAINER LANDING TABLE
# -------------------------------
resource "aws_glue_catalog_table" "step_trainer_landing" {
  name          = "step_trainer_landing"
  database_name = aws_glue_catalog_database.stedi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification     = "json"
    has_encrypted_data = "false"
  }

  storage_descriptor {
    location      = "s3://${var.datalake_bucket_name}/step_trainer_landing/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "sensorReadingTime"
      type = "bigint"
    }
    columns {
      name = "serialNumber"
      type = "string"
    }
    columns {
      name = "distanceFromObject"
      type = "int"
    }
  }
}

# STEP TRAINER TRUSTED JOB
resource "aws_glue_job" "step_trainer_trusted" {
  name     = "step_trainer_trusted"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/step_trainer_trusted.py"
  }

  glue_version = "4.0"
  timeout      = 10
  max_retries  = 0

  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }

  depends_on = [
    aws_glue_catalog_table.step_trainer_landing,
    aws_glue_catalog_table.customer_curated
  ]
}

# -------------------------------
# FINAL JOIN JOB (Curated)
# -------------------------------
resource "aws_glue_job" "machine_learning_curated" {
  name     = "machine_learning_curated"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.datalake_bucket_name}/scripts/machine_learning_curated.py"
  }

  glue_version = "4.0"
  timeout      = 10
  max_retries  = 0

  default_arguments = {
    "--TempDir"                 = "s3://${var.datalake_bucket_name}/temp/"
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
  }

  depends_on = [
    aws_glue_job.customer_trusted,
    aws_glue_job.accelerometer_trusted,
    aws_glue_job.step_trainer_trusted
  ]
}
