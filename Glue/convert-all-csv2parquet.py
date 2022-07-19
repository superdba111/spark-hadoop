import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = 'einc-datalake-all-s3-dev'
prefix = 'raw/eblock/'

s3 = boto3.client("s3")
result = s3.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
folder_list = []
for o in result.get('CommonPrefixes'):
    # print(o.get('Prefix').split('/')[-2])
    folder_list.append(o.get('Prefix').split('/')[-2])
    
for folder in folder_list:

    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": "|",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": ["s3://einc-datalake-all-s3-dev/raw/eblock/"+folder+"/"],
            "recurse": True,
        },
        transformation_ctx="S3bucket_node1",
    )

    # Script generated for node S3 bucket
    S3bucket_node3 = glueContext.getSink(
        path="s3://einc-datalake-all-s3-dev/clean/eblock/"+folder+"/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="S3bucket_node3",
    )
    S3bucket_node3.setCatalogInfo(
        catalogDatabase="eblock-s3-parquet-clean", catalogTableName=folder
    )
    S3bucket_node3.setFormat("glueparquet")
    S3bucket_node3.writeFrame(S3bucket_node1)
    job.commit()

