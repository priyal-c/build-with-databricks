# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/sfo_customer_survey

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS reatil_scratch
# MAGIC COMMENT 'This is retail_schema' 
# MAGIC LOCATION 's3://data-lakehouse-delta/retail_db_scratch'

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS reatil_scratch_ap
# MAGIC COMMENT 'This is retail_schema' 
# MAGIC LOCATION 's3://databricks-readonly-tniz8qxin7584i9ox3gi4p7fhcq3wuse1b-s3alias/retail_db_scratch_ap'

# COMMAND ----------

# Define the input and output formats and paths and the table name.
read_format = 'csv'
write_format = 'delta'
load_path = 'dbfs:/databricks-datasets/online_retail/data-001/data.csv'
save_path = 's3://databricks-readonly-tniz8qxin7584i9ox3gi4p7fhcq3wuse1b-s3alias/retail_db_scratch_ap_mod/retail'
table_name = 'reatil_scratch_ap.retail_mod'

# Load the data from its source.
retail = spark \
  .read \
  .format(read_format) \
  .option("header", "true") \
  .load(load_path)

# Write the data to its target.
retail.write \
  .format(write_format) \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# Define the input and output formats and paths and the table name.
read_format = 'csv'
write_format = 'delta'
load_path = 'dbfs:/databricks-datasets/online_retail/data-001/data.csv'
save_path = 's3://data-lakehouse-delta/retail_db_scratch/retail'
table_name = 'reatil_scratch.retail'

# Load the data from its source.
retail = spark \
  .read \
  .format(read_format) \
  .option("header", "true") \
  .load(load_path)

# Write the data to its target.
retail.write \
  .format(write_format) \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reatil_scratch.retail

# COMMAND ----------

# Define the input and output formats and paths and the table name.
read_format = 'csv'
write_format = 'delta'
load_path = 'dbfs:/databricks-datasets/sfo_customer_survey/2013_SFO_Customer_Survey.csv'
save_path = 's3://data-lakehouse-delta/retail_db_scratch/cs_survey'
table_name = 'reatil_scratch.cs_survey'

# Load the data from its source.
survey = spark \
  .read \
  .format(read_format) \
  .option("header", "true") \
  .option("inferschema","true") \
  .load(load_path)

# Write the data to its target.
survey.write \
  .format(write_format) \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reatil_scratch.cs_survey

# COMMAND ----------

# MAGIC %md ## Data seeding- copy the sample credit card fraud dataset to your s3 bucket

# COMMAND ----------

# MAGIC %fs cp -r /databricks-datasets/credit-card-fraud s3://data-lakehouse-delta/credit-card-fraud

# COMMAND ----------

# MAGIC %fs ls s3://data-lakehouse-delta/credit-card-fraud/data

# COMMAND ----------

# MAGIC %md ## validate the list (read-only) acccess on the s3 access point

# COMMAND ----------

# MAGIC %fs ls s3://databricks-readonly-tniz8qxin7584i9ox3gi4p7fhcq3wuse1b-s3alias/credit-card-fraud/data

# COMMAND ----------

# MAGIC %md ## the read only access works via access point, but the write access is denied

# COMMAND ----------

# MAGIC %fs put s3://databricks-readonly-tniz8qxin7584i9ox3gi4p7fhcq3wuse1b-s3alias/credit-card-fraud/data/new_file3.txt "This is a file in cloud storage."

# COMMAND ----------

# MAGIC %md ## Lets validate that Databricks application can not access the s3 bucket directly ( outside of Databricks specific access point)

# COMMAND ----------

# MAGIC %fs ls s3://data-lakehouse-delta/credit-card-fraud

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reatil_scratch.retail

# COMMAND ----------

# MAGIC %sql
# MAGIC update reatil_scratch.retail
# MAGIC set 
# MAGIC Description = "credit card account 1111-0000-1111-0008"
# MAGIC where InvoiceNo= "536423" and StockCode="22113" and CustomerID = '18085'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reatil_scratch.retail
# MAGIC where InvoiceNo= "536423" and StockCode="22113" and CustomerID = '18085'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table reatil_scratch.retail_temp
# MAGIC as
# MAGIC select *,SSN from reatil_scratch.retail

# COMMAND ----------


read_format = 'csv'
write_format = 'delta'
load_path = 'dbfs:/databricks-datasets/online_retail/data-001/data.csv'
save_path = 's3://databricks-pii-object-lambda-accesspoint/retail_db_scratch_pii/retail'
table_name = 'reatil_scratch.retail_pii'

# Load the data from its source.
retail = spark \
  .read \
  .format(read_format) \
  .option("header", "true") \
  .load(load_path)

# Write the data to its target.
retail.write \
  .format(write_format) \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")


# COMMAND ----------


