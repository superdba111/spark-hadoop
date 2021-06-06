Principle--fast and cheap based on the project requirement, also show more aws service such as lambda

Phase 1, prepare data source by the univesal solution
There are many ways to import data to RDS (SQL) and NOSQL Dynamodb
But I prefer to use s3 service as the landing zone due to aws cloud ecosystem and data life cycle's process

step 1, run loadData2S3 to load raw data to S3, both json and csv files

step 2, run lambda_function on aws to import json data to nosql dynamodb;
        trigger--new json file

step 3, use free tier rds postgresql service 
        use the copy command to load s3 data to postgresql


Phase 2: build the simple data pippeline from dynamodb and postgresql to redshift

step 1, create redshift cluster free tier, got available zone issue with other aws services since no option on 
free tier on zone.
``` there are serveral ways to handle the dataflow from dynamodb to redshit 
    DynamoDB to Redshift: Using Redshift’s COPY command
This is by far the simplest way to copy a DynamoDB table to Redshift. Redshift’s COPY command can accept a DynamoDB URL as one of the inputs and manage the copying process on its own. The syntax for the COPY command is as below. 

copy <target_tablename> from 'dynamodb://<source_table_name>'
authorization
read ratio '<integer>';
this is good for full data loading.

But I prefer to use glue as etl since it is flexible and powerful for future usages such as mapping and transforming on data frame, also it is easily scheduled

```
step 2, create glue connection for rds postgresql and redshift, then crawler dynamodb create catalog  db/table

step 3, create two glue jobs, autogenrate scripts, then modify them, run them, verify data are in redshift

step 4, use glue worksflows schedule two paralle jobs, make sure "Job bookmark" on both glue jobs--bookmark for incremental jobs, it works when data either inserted or updated, avoid full loading.

There are many ways to handle the process, one nice thing is to use DMS service to sync data between dynamodb/postgresql and redshift, CDC avoid scheduling and using glue, this is a database level solution. No coding.

there is other option for cicd--airflow can make all process automatically, using lambda to trigger glue jobs etc





