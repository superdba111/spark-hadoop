
drop table public.txns
create table public.txns (
	Account_No bigint,
	DATE1 date,
	TRANSACTION_DETAILS varchar(200),
	CHIP_USED varchar(5),
	VALUE_DATE date, 
	WITHDRAWAL_AMT varchar(100), 
	DEPOSIT_AMT varchar(100),
	BALANCE_AMT varchar(100)
)

select * from public.txns;

SELECT aws_s3.table_import_from_s3 (
  'public.txns',  -- the table where you want the data to be imported
  '', -- column list. Empty means to import everything
  '(FORMAT csv, HEADER true, DELIMITER '','', QUOTE ''"'', ESCAPE ''\'')', -- this is what I use to import standard CSV
  'stori', -- the bucket name and ONLY the bucket name, without anything else
  'txns.csv', -- the path from the bucket to the file. Does not have to be gz
  'us-west-2' -- the region where the bucket is
);

