import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "dynamo", table_name = "trades", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "dynamo", table_name = "trades", transformation_ctx = "datasource0")
## or directly connect dynamodb
# dyf = glueContext.create_dynamic_frame.from_options(
#     connection_type="dynamodb",
#     connection_options= {"dynamodb.input.tableName": "trades"}
# )
## @type: ApplyMapping
## @args: [mapping = [("shares", "long", "shares", "long"), ("ticker", "string", "ticker", "string"), ("ticket", "string", "ticket", "string"), ("price", "long", "price", "long"), ("details.lag", "long", "`details.lag`", "long"), ("details.system", "string", "`details.system`", "string"), ("details.asks", "array", "`details.asks`", "string"), ("details.bids", "array", "`details.bids`", "string"), ("time.date", "string", "`time.date`", "string"), ("id", "string", "id", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("shares", "long", "shares", "long"), ("ticker", "string", "ticker", "string"), ("ticket", "string", "ticket", "string"), ("price", "long", "price", "long"), ("details.lag", "long", "`details.lag`", "long"), ("details.system", "string", "`details.system`", "string"), ("details.asks", "array", "`details.asks`", "string"), ("details.bids", "array", "`details.bids`", "string"), ("time.date", "string", "`time.date`", "string"), ("id", "string", "id", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "reddb", connection_options = {"dbtable": "trades", "database": "stori"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "reddb", connection_options = {"dbtable": "trades", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()