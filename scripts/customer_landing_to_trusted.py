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
# Data Quality Ruleset (basic rule)
# -----------------------------------------------
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""

# -----------------------------------------------
# Read from customer_landing in Data Catalog
# -----------------------------------------------
customer_landing_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="customer_landing",
    transformation_ctx="customer_landing_df"
)

# -----------------------------------------------
# Filter rows with consent
# -----------------------------------------------
query = """
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
  AND shareWithResearchAsOfDate != 0
"""

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

filtered_df = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"customer_landing": customer_landing_df},
    transformation_ctx="filtered_df"
)

# -----------------------------------------------
# Apply schema mapping (if needed)
# -----------------------------------------------
mapped_df = ApplyMapping.apply(
    frame=filtered_df,
    mappings=[
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long")
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
        "dataQualityEvaluationContext": "dq_context",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# -----------------------------------------------
# Write to customer_trusted in S3 and register in Glue
# -----------------------------------------------
sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="customer_trusted"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(mapped_df)

job.commit()
