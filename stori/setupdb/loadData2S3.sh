#!/bin/bash
#aws dynamodb batch-write-item --request-items file://trades.json--diff way
#using universal solution
aws s3 cp txns.csv  s3://stori/data/
aws s3 cp trades.json  s3://stori/data/
