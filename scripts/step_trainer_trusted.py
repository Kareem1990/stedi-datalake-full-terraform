import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node StepTrainerSource
StepTrainerSource_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerSource_node"
)

# Script generated for node CustomerCuratedSource
CustomerCuratedSource_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedSource_node"
)

# ✅ FIXED SQL Join Case Sensitivity Issue
SqlQuery = '''
SELECT StepTrainerSource.*
FROM StepTrainerSource
JOIN CustomerCuratedSource
  ON StepTrainerSource.serialNumber = CustomerCuratedSource.serialnumber
'''

Joined_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "StepTrainerSource": StepTrainerSource_node,
        "CustomerCuratedSource": CustomerCuratedSource_node
    },
    transformation_ctx="Joined_node"
)

Mapped_node = ApplyMapping.apply(
    frame=Joined_node,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int")
    ],
    transformation_ctx="Mapped_node"
)

EvaluateDataQuality().process_rows(
    frame=Mapped_node,
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

sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedSink_node"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="step_trainer_trusted"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(Mapped_node)

job.commit()
