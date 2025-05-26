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

# Script generated for node CustomerTrustedSource
CustomerTrustedSource_node1748118614029 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="customer_trusted", transformation_ctx="CustomerTrustedSource_node1748118614029")

# Script generated for node AccelerometerTrustedSource
AccelerometerTrustedSource_node1748118653430 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedSource_node1748118653430")

# Script generated for node JoinCustomerAccelerometer
SqlQuery0 = '''
SELECT DISTINCT CustomerTrustedSource.*
FROM CustomerTrustedSource
JOIN AccelerometerTrustedSource
  ON CustomerTrustedSource.email = AccelerometerTrustedSource.user
'''
JoinCustomerAccelerometer_node1748118704121 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"CustomerTrustedSource":CustomerTrustedSource_node1748118614029, "AccelerometerTrustedSource":AccelerometerTrustedSource_node1748118653430}, transformation_ctx = "JoinCustomerAccelerometer_node1748118704121")

# Script generated for node ApplyMapping
ApplyMapping_node1748118836395 = ApplyMapping.apply(frame=JoinCustomerAccelerometer_node1748118704121, mappings=[("customerName", "string", "customerName", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthDay", "string", "birthDay", "string"), ("serialNumber", "string", "serialNumber", "string"), ("registrationDate", "bigint", "registrationDate", "long"), ("lastUpdateDate", "bigint", "lastUpdateDate", "long"), ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"), ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"), ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long")], transformation_ctx="ApplyMapping_node1748118836395")

# Script generated for node CustomerCuratedSink
EvaluateDataQuality().process_rows(frame=ApplyMapping_node1748118836395, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748117379750", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCuratedSink_node1748118922415 = glueContext.getSink(path="s3://stedi-datalake-terraform/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedSink_node1748118922415")
CustomerCuratedSink_node1748118922415.setCatalogInfo(catalogDatabase="stedi_lake",catalogTableName="customer_curated")
CustomerCuratedSink_node1748118922415.setFormat("glueparquet", compression="snappy")
CustomerCuratedSink_node1748118922415.writeFrame(ApplyMapping_node1748118836395)
job.commit()