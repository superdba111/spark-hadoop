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
## @args: [database = "pgsdb", table_name = "stori_public_txns", transformation_ctx = "datasource0", additional_options = {"jobBookmarkKeys":["id"],"jobBookmarkKeysSortOrder":"asc"}]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "pgsdb", table_name = "stori_public_txns", transformation_ctx = "datasource0", additional_options = {"jobBookmarkKeys":["customerid"],"jobBookmarkKeysSortOrder":"asc"})
## @type: ApplyMapping
## @args: [mapping = [("date1", "date", "date1", "date"), ("deposit_amt", "string", "deposit_amt", "string"), ("account_no", "string", "account_no", "string"), ("withdrawal_amt", "string", "withdrawal_amt", "string"), ("transaction_details", "string", "transaction_details", "string"), ("chip_used", "string", "chip_used", "string"), ("value_date", "date", "value_date", "date"), ("balance_amt", "string", "balance_amt", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date1", "date", "date1", "date"), ("deposit_amt", "string", "deposit_amt", "string"), ("account_no", "string", "account_no", "string"), ("withdrawal_amt", "string", "withdrawal_amt", "string"), ("transaction_details", "string", "transaction_details", "string"), ("chip_used", "string", "chip_used", "string"), ("value_date", "date", "value_date", "date"), ("balance_amt", "string", "balance_amt", "string")], transformation_ctx = "applymapping1")
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
## @args: [catalog_connection = "reddb", connection_options = {"dbtable": "stori_public_txns", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "reddb", connection_options = {"dbtable": "stori_public_txns", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()