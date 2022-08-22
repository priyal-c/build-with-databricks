# Databricks notebook source

dbutils.fs.mkdirs("s3://data-lakehouse-delta/customer_data")

# COMMAND ----------

# MAGIC %fs ls s3://data-lakehouse-delta/customer_data

# COMMAND ----------

dbutils.fs.head("s3://databricks-pii-acces-md1q8psqrzwimo4sq81rnkh1eqmb4use1b-s3alias/customer_data/ tutorial.txt", 100000000)

# COMMAND ----------

# MAGIC %fs ls s3://databricks-pii-acces-md1q8psqrzwimo4sq81rnkh1eqmb4use1b-s3alias/customer_data

# COMMAND ----------

import pandas as ps
ps.read_csv(s3_client.get_object(
    Bucket='arn:aws:s3-object-lambda:us-east-1:630817809197:accesspoint/databricks-pii-object-lambda-accesspoint',
    Key='Customerinfo.csv'
)['Body'])
    

# COMMAND ----------

from pyspark.pandas import read_csv
read_csv('arn:aws:s3-object-lambda:us-east-1:630817809197:accesspoint/databricks-pii-object-lambda-accesspoint/Customerinfo.csv')

# COMMAND ----------

from pyspark.pandas import read_csv
read_csv('s3://data-lakehouse-delta/customer_data/Customerinfo.csv')

# COMMAND ----------


