-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze_customers_reviews
COMMENT "The customers reviews on Products raw data"
TBLPROPERTIES ("quality" = "bronze")
PARTITIONED BY (product_category)
AS SELECT * FROM cloud_files("s3://amazon-reviews-pds/parquet/",
 "parquet",
map("schema","marketplace STRING,
          product_category STRING,
          customer_id STRING, 
          review_id STRING, 
          product_id STRING, 
          product_parent STRING, 
          product_title STRING, 
          star_rating INT, 
          helpful_votes INT, 
          total_votes INT, 
          vine STRING, 
          verified_purchase STRING, 
          review_headline STRING, 
          review_body STRING, 
          review_date BIGINT, 
          year INT"))


-- COMMAND ----------

-- sliver level table  to enriched with sentiment information by calling Amazon Comprehend UDF

CREATE STREAMING LIVE TABLE sliver_customers_reviews
PARTITIONED BY (product_category)
COMMENT "This is a enriched table with Sentiment added against each product review"
TBLPROPERTIES ("quality" = "silver")
AS
select a.* , 
db_detect_sentiment_udf(a.review_body) as sentiment -- Amazon Comprehend Detect Sentiment UDF call
from STREAM(LIVE.bronze_customers_reviews) a
WHERE a.year in (2000,2001,2002) and a.product_category = "Health_&_Personal_Care"



-- COMMAND ----------

-- Gold level table for product with Negative reviews in USA region
CREATE STREAMING LIVE TABLE gold_negative_customers_reviews_usa
PARTITIONED BY (created_date)
COMMENT "This is a gold level table having records of reveiws with Negative Sentiment in US region"
TBLPROPERTIES ("quality" = "gold")
AS
select a.* , current_date() as created_date
from STREAM(LIVE.sliver_customers_reviews) a
where sentiment = "NEGATIVE" and  a.marketplace = 'US'
