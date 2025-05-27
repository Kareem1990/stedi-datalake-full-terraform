import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Initialize Spark and Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Data Catalog table: customer_trusted
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lake",
    table_name="customer_trusted",
    transformation_ctx="customer_df"
)

# Optional: apply minimal mapping (or skip if not needed)
mapped_df = ApplyMapping.apply(
    frame=customer_df,
    mappings=[
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("registrationDate", "bigint", "registrationDate", "bigint"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint")
    ],
    transformation_ctx="mapped_df"
)

# Write to S3 as customer_curated
sink = glueContext.getSink(
    path="s3://stedi-datalake-terraform-kr/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_lake",
    catalogTableName="customer_curated"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(mapped_df)

job.commit()
