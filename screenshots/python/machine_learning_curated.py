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

# Script generated for node StepTrainerTrustedSource
StepTrainerTrustedSource_node1748124306379 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedSource_node1748124306379")

# Script generated for node AccelerometerTrustedSource
AccelerometerTrustedSource_node1748124260691 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedSource_node1748124260691")

# Script generated for node JoinStepTrainerWithCustomer
SqlQuery0 = '''
SELECT DISTINCT
  a.user,
  a.timestamp,
  a.x,
  a.y,
  a.z,
  s.sensorreadingtime,
  s.serialnumber
FROM
  StepTrainerTrustedSource s
JOIN
  AccelerometerTrustedSource a
ON
  s.sensorreadingtime = a.timestamp
'''
JoinStepTrainerWithCustomer_node1748124354807 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"StepTrainerTrustedSource":StepTrainerTrustedSource_node1748124306379, "AccelerometerTrustedSource":AccelerometerTrustedSource_node1748124260691}, transformation_ctx = "JoinStepTrainerWithCustomer_node1748124354807")

# Script generated for node Change Schema
ChangeSchema_node1748125317599 = ApplyMapping.apply(frame=JoinStepTrainerWithCustomer_node1748124354807, mappings=[("user", "string", "user", "string"), ("timestamp", "bigint", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double"), ("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string")], transformation_ctx="ChangeSchema_node1748125317599")

# Script generated for node MachineLearningCuratedSink
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1748125317599, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748123518510", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCuratedSink_node1748125390108 = glueContext.getSink(path="s3://stedi-datalake-terraform/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCuratedSink_node1748125390108")
MachineLearningCuratedSink_node1748125390108.setCatalogInfo(catalogDatabase="stedi_lake",catalogTableName="machine_learning_curated")
MachineLearningCuratedSink_node1748125390108.setFormat("glueparquet", compression="snappy")
MachineLearningCuratedSink_node1748125390108.writeFrame(ChangeSchema_node1748125317599)
job.commit()