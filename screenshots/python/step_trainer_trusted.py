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
StepTrainerSource_node1748120627474 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="step_trainer_landing", transformation_ctx="StepTrainerSource_node1748120627474")

# Script generated for node CustomerCuratedSource
CustomerCuratedSource_node1748120688971 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="customer_curated", transformation_ctx="CustomerCuratedSource_node1748120688971")

# Script generated for node JoinStepTrainerWithCustomerCurated
SqlQuery0 = '''
SELECT StepTrainerSource.*
FROM StepTrainerSource
JOIN CustomerCuratedSource
  ON StepTrainerSource.serialNumber = CustomerCuratedSource.serialNumber
'''
JoinStepTrainerWithCustomerCurated_node1748120725675 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"StepTrainerSource":StepTrainerSource_node1748120627474, "CustomerCuratedSource":CustomerCuratedSource_node1748120688971}, transformation_ctx = "JoinStepTrainerWithCustomerCurated_node1748120725675")

# Script generated for node ApplyMappingStepTrainerTrusted
ApplyMappingStepTrainerTrusted_node1748120959452 = ApplyMapping.apply(frame=JoinStepTrainerWithCustomerCurated_node1748120725675, mappings=[("sensorReadingTime", "bigint", "sensorReadingTime", "long"), ("serialNumber", "string", "serialNumber", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="ApplyMappingStepTrainerTrusted_node1748120959452")

# Script generated for node StepTrainerTrustedSink
EvaluateDataQuality().process_rows(frame=ApplyMappingStepTrainerTrusted_node1748120959452, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748120539484", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedSink_node1748121446515 = glueContext.getSink(path="s3://stedi-datalake-terraform/step_trainer_trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedSink_node1748121446515")
StepTrainerTrustedSink_node1748121446515.setCatalogInfo(catalogDatabase="stedi_lake",catalogTableName="step_trainer_trusted")
StepTrainerTrustedSink_node1748121446515.setFormat("glueparquet", compression="snappy")
StepTrainerTrustedSink_node1748121446515.writeFrame(ApplyMappingStepTrainerTrusted_node1748120959452)
job.commit()