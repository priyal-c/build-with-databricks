# Databricks notebook source
from pyspark.sql.functions import udf
import boto3
from botocore.exceptions import ClientError
import logging

region = spark.conf.get('spark.databricks.clusterUsageTags.dataPlaneRegion') 

# COMMAND ----------

# UDF to Detect Sentiment using Amazon Comprehend
def db_detect_sentiment(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
      response = comprehend.detect_sentiment(Text=text, LanguageCode='en') # Language can can be pass dynamically by calling db_detect_language_udf 
  except ClientError:
      logger.exception("Couldn't detect sentiment.")
      raise
  else:
      return response['Sentiment']


spark.udf.register("db_detect_sentiment_udf", db_detect_sentiment) #register as UDF


# COMMAND ----------

# UDF to Detect Language using Amazon Comprehend
def db_detect_language(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
    response = comprehend.detect_dominant_language(Text=text)
    languages = response['Languages']
    logger.info("Detected %s languages.", len(languages))
  except ClientError:
    logger.exception("Couldn't detect languages.")
    raise
  else:
    return languages


spark.udf.register("db_detect_language_udf", db_detect_language) #register as UDF

# COMMAND ----------

# UDF to Detect Entities using Amazon Comprehend
def db_detect_entities(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
    response = comprehend.detect_entities(Text=text, LanguageCode='en') # Language can can be pass dynamically by calling db_detect_language_udf 
    entities = response['Entities']
    logger.info("Detected %s entities.", len(entities))
  except ClientError:
    logger.exception("Couldn't detect entities.")
    raise
  else:
    return entities


spark.udf.register("db_detect_entities_udf", db_detect_entities) #register as UDF

# COMMAND ----------

# UDF to Detect Key phrases using Amazon Comprehend
def db_detect_key_phrases(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
    response = comprehend.detect_key_phrases(Text=text, LanguageCode='en') # Language can can be pass dynamically by calling db_detect_language_udf 
    phrases = response['KeyPhrases']
    logger.info("Detected %s phrases.", len(phrases))
  except ClientError:
    logger.exception("Couldn't detect phrases.")
    raise
  else:
    return phrases


spark.udf.register("db_detect_key_phrases_udf", db_detect_key_phrases) #register as UDF

# COMMAND ----------

# UDF to Detect PII using Amazon Comprehend
def db_detect_pii(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
    response = comprehend.detect_pii_entities(Text=text, LanguageCode='en') # Language can can be pass dynamically by calling db_detect_language_udf 
    entities = response['Entities']
    logger.info("Detected %s PII entities.", len(entities))
  except ClientError:
    logger.exception("Couldn't detect PII entities.")
    raise
  else:
    return entities


spark.udf.register("db_detect_pii_udf", db_detect_pii) #register as UDF

# COMMAND ----------

# UDF to Detect Sytanx using Amazon Comprehend
def db_detect_syntax(text):
  logger = logging.getLogger(__name__)
  comprehend = boto3.client('comprehend', region_name=region)
  try:
    response = comprehend.detect_syntax(Text=text, LanguageCode='en') # Language can can be pass dynamically by calling db_detect_language_udf 
    tokens = response['SyntaxTokens']
    logger.info("Detected %s syntax tokens.", len(tokens))
  except ClientError:
    logger.exception("Couldn't detect syntax.")
    raise
  else:
    return tokens


spark.udf.register("db_detect_syntax_udf", db_detect_syntax) #register as UDF
