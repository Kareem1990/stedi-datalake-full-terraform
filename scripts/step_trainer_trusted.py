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
# Read from Catalog: step_trainer_landing + customer_curated
# -----------------------------------------------
step_trainer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_df"
)

customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="customer_curated",
    transformation_ctx="customer_curated_df"
)

# -----------------------------------------------
# Join both datasets on serialNumber
# -----------------------------------------------
query = """
SELECT s.*
FROM step_trainer_df s
JOIN customer_curated_df c
ON s.serialNumber = c.serialNumber
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
        "step_trainer_df": step_trainer_df,
        "customer_curated_df": customer_curated_df
    },
    transformation_ctx="joined_df"
)

# -----------------------------------------------
# Apply Schema Mapping
# -----------------------------------------------
mapped_df = ApplyMapping.apply(
    frame=joined_df,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int")
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
        "dataQualityEvaluationContext": "dq_step_trainer",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# -----------------------------------------------
# Write to Trusted Zone + Register in Catalog
# -----------------------------------------------
sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="step_trainer_trusted"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(mapped_df)

job.commit()
