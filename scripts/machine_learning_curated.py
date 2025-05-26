import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

# -----------------------------------------------
# Initialize Glue Job
# -----------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------------------------
# Basic Data Quality Rules
# -----------------------------------------------
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""

# -----------------------------------------------
# Read from Trusted Zones
# -----------------------------------------------
accelerometer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_df"
)

step_trainer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_df"
)

# -----------------------------------------------
# Join both datasets on timestamp / sensor time
# -----------------------------------------------
query = """
SELECT DISTINCT
  a.user,
  a.timestamp,
  a.x,
  a.y,
  a.z,
  s.sensorreadingtime,
  s.serialnumber
FROM
  step_trainer_trusted_df s
JOIN
  accelerometer_trusted_df a
ON
  s.sensorreadingtime = a.timestamp
"""

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

joined_df = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={
        "step_trainer_trusted_df": step_trainer_trusted_df,
        "accelerometer_trusted_df": accelerometer_trusted_df
    },
    transformation_ctx="joined_df"
)

# -----------------------------------------------
# Apply Schema Mapping
# -----------------------------------------------
mapped_df = ApplyMapping.apply(
    frame=joined_df,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "bigint", "timestamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
        ("sensorreadingtime", "bigint", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber", "string")
    ],
    transformation_ctx="mapped_df"
)

# -----------------------------------------------
# Evaluate Data Quality (Optional)
# -----------------------------------------------
EvaluateDataQuality().process_rows(
    frame=mapped_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_curated",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# -----------------------------------------------
# Write to curated output
# -----------------------------------------------
sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="ml_curated_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="machine_learning_curated"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(mapped_df)

job.commit()
