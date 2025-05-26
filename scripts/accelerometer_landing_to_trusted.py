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
# Read from Data Catalog: customer_trusted + accelerometer_landing
# -----------------------------------------------
customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_df"
)

accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_df"
)

# -----------------------------------------------
# Join: keep accelerometer rows for customers with consent
# -----------------------------------------------
query = """
SELECT a.*
FROM accelerometer_df a
JOIN customer_trusted_df c
ON a.user = c.email
"""

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

filtered_df = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={
        "accelerometer_df": accelerometer_df,
        "customer_trusted_df": customer_trusted_df
    },
    transformation_ctx="filtered_df"
)

# -----------------------------------------------
# Apply Schema Mapping
# -----------------------------------------------
mapped_df = ApplyMapping.apply(
    frame=filtered_df,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "bigint", "timestamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double")
    ],
    transformation_ctx="mapped_df"
)

# -----------------------------------------------
# Optional: Evaluate Data Quality
# -----------------------------------------------
EvaluateDataQuality().process_rows(
    frame=mapped_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_accelerometer",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# -----------------------------------------------
# Write to accelerometer_trusted in S3 and Glue
# -----------------------------------------------
sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="accelerometer_trusted"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(mapped_df)

job.commit()
