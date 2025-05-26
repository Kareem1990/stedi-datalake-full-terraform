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
CustomerTrustedSource_node1748111021158 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="customer_trusted", transformation_ctx="CustomerTrustedSource_node1748111021158")

# Script generated for node AccelerometerSource
AccelerometerSource_node1748108163166 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="accelerometer_landing", transformation_ctx="AccelerometerSource_node1748108163166")

# Script generated for node FilterByCustomerConsent
SqlQuery0 = '''
SELECT AccelerometerSource.*
FROM AccelerometerSource
JOIN CustomerTrustedSource
  ON AccelerometerSource.user = CustomerTrustedSource.email

'''
FilterByCustomerConsent_node1748111058569 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"AccelerometerSource":AccelerometerSource_node1748108163166, "CustomerTrustedSource":CustomerTrustedSource_node1748111021158}, transformation_ctx = "FilterByCustomerConsent_node1748111058569")

# Script generated for node ApplyAccelerometerSchema
ApplyAccelerometerSchema_node1748111586085 = ApplyMapping.apply(frame=FilterByCustomerConsent_node1748111058569, mappings=[("user", "string", "user", "string"), ("timestamp", "bigint", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ApplyAccelerometerSchema_node1748111586085")

# Script generated for node AccelerometerTrustedSink
EvaluateDataQuality().process_rows(frame=ApplyAccelerometerSchema_node1748111586085, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748108127681", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedSink_node1748111644064 = glueContext.getSink(path="s3://stedi-datalake-terraform/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedSink_node1748111644064")
AccelerometerTrustedSink_node1748111644064.setCatalogInfo(catalogDatabase="stedi_lake",catalogTableName="accelerometer_trusted")
AccelerometerTrustedSink_node1748111644064.setFormat("glueparquet", compression="snappy")
AccelerometerTrustedSink_node1748111644064.writeFrame(ApplyAccelerometerSchema_node1748111586085)
job.commit()