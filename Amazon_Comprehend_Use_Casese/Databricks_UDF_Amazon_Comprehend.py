# Databricks notebook source
from pyspark.sql.functions import udf
import boto3
from botocore.exceptions import ClientError
import logging

region = spark.conf.get('spark.databricks.clusterUsageTags.dataPlaneRegion')

# COMMAND ----------

# UDF to Detect Sentiment using Amazon Comprehend
def detect_sentiment(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
      response = comprehend.detect_sentiment(Text=text, LanguageCode='en') 
  except ClientError:
      logger.exception("Couldn't detect sentiment.")
      raise
  else:
      return response['Sentiment']


spark.udf.register("detect_sentiment_udf", detect_sentiment)

