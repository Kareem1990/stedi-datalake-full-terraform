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

# Script generated for node CustomerLandingSource
CustomerLandingSource_node1748103628849 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lake", table_name="customer_landing", transformation_ctx="CustomerLandingSource_node1748103628849")

# Script generated for node FilterByConsentSQL
SqlQuery0 = '''
SELECT *
FROM CustomerLandingSource
WHERE sharewithresearchasofdate IS NOT NULL
  AND sharewithresearchasofdate != 0
'''
FilterByConsentSQL_node1748103848587 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"CustomerLandingSource":CustomerLandingSource_node1748103628849}, transformation_ctx = "FilterByConsentSQL_node1748103848587")

# Script generated for node FilterByConsentSQL
FilterByConsentSQL_node1748105669151 = ApplyMapping.apply(frame=FilterByConsentSQL_node1748103848587, mappings=[("customerName", "string", "customerName", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthDay", "string", "birthDay", "string"), ("serialNumber", "string", "serialNumber", "string"), ("registrationDate", "bigint", "registrationDate", "long"), ("lastUpdateDate", "bigint", "lastUpdateDate", "long"), ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"), ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"), ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long")], transformation_ctx="FilterByConsentSQL_node1748105669151")

# Script generated for node CustomerTrustedSink
EvaluateDataQuality().process_rows(frame=FilterByConsentSQL_node1748105669151, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748103348495", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedSink_node1748105752041 = glueContext.getSink(path="s3://stedi-datalake-terraform/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedSink_node1748105752041")
CustomerTrustedSink_node1748105752041.setCatalogInfo(catalogDatabase="stedi_lake",catalogTableName="customer_trusted")
CustomerTrustedSink_node1748105752041.setFormat("glueparquet", compression="snappy")
CustomerTrustedSink_node1748105752041.writeFrame(FilterByConsentSQL_node1748105669151)
job.commit()