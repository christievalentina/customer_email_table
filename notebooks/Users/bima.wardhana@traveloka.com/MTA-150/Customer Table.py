# Databricks notebook source
# Parameters are passed from pipeline executor (calling the information from Google Sheet)
# The code's are generalized to the degree of new transaction from which product issuance data being inserted/updated to customer unique email data of which product
product = dbutils.widgets.get("product")
unique_email_collection = dbutils.widgets.get("unique_email_collection")
product_issuance_data_path = dbutils.widgets.get("product_issuance_data_path")
customer_unique_email_data_path = dbutils.widgets.get("customer_unique_email_data_path")
update_with_n_latest_date = dbutils.widgets.get("update_with_n_latest_date")
acquired_from_info = dbutils.widgets.get("acquired_from_info")

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

# Get yesterday date for getting yesterday's snapshot data of customer unique email
yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
yesterday_date = "{:02d}-{:02d}-{:02d}".format(yesterday.year, yesterday.month, yesterday.day)

# Get date from which issuance data will be used to update customer unique email data (based on parameter update_with_n_latest_date)
run_from = datetime.datetime.now() - datetime.timedelta(days = update_with_n_latest_date)
run_from_date = "{:02d}-{:02d}-{:02d}".format(run_from.year, run_from.month, run_from.day)

# Loading (& Filter) Data
product_issuance_data = spark.read.parquet(product_issuance_data_path).filter(to_date(from_unixtime(col("timestamp")/1000 + (7 * 3600))) >= run_from_date)
customer_unique_email_data = spark.read.parquet(customer_unique_email_data_path).filter(col("snapshot_date") == yesterday_date)

# Creating Aliases
product_issuance_data.createOrReplaceTempView("product_issuance_data")
customer_unique_email_data.createOrReplaceTempView("customer_unique_email_data")

# COMMAND ----------

updated_customer_master_data = spark.sql("""
                                         SELECT
                                              -- Append contact email that has not been listed on customer master table
                                              COALESCE(customer_unique_email_data.contact_email, product_issuance_data.email) AS contact_email,

                                              -- Handling first_product_issuance_data_timestamp and booking_id value (for each contact e-mail): 
                                              -- 1) if contact e-mail has not been listed, 
                                              -- 2) if contact-email has been listed but the listed timestamp is more recent than the product_issuance_data one

                                              -- First product_issuance_data Timestamp
                                              CASE
                                                WHEN customer_unique_email_data.first_product_issuance_data_timestamp IS NULL 
                                                  THEN product_issuance_data.timestamp
                                                WHEN product_issuance_data.timestamp IS NULL 
                                                  THEN customer_unique_email_data.first_product_issuance_data_timestamp
                                                WHEN customer_unique_email_data.first_product_issuance_data_timestamp <= product_issuance_data.timestamp 
                                                  THEN customer_unique_email_data.first_product_issuance_data_timestamp
                                                ELSE product_issuance_data.timestamp
                                              END AS first_product_issuance_data_timestamp,

                                              -- First product_issuance_data Booking ID
                                              CASE
                                                WHEN customer_unique_email_data.booking_id IS NULL 
                                                  THEN product_issuance_data.bookingId
                                                WHEN product_issuance_data.bookingId IS NULL 
                                                  THEN customer_unique_email_data.booking_id
                                                WHEN customer_unique_email_data.booking_id <= product_issuance_data.bookingId 
                                                  THEN customer_unique_email_data.booking_id
                                                ELSE customer_unique_email_data.booking_id
                                              END AS first_product_issuance_data_booking_id,

                                              -- First product_issuance_data Acquired From
                                              CASE
                                                WHEN customer_unique_email_data.first_product_issuance_data_timestamp IS NULL 
                                                    THEN """ + acquired_from_info + """
                                                WHEN product_issuance_data.timestamp IS NULL 
                                                    THEN customer_unique_email_data.acquired_from
                                                WHEN customer_unique_email_data.first_product_issuance_data_timestamp <= product_issuance_data.timestamp 
                                                    THEN customer_unique_email_data.acquired_from
                                                ELSE """ + acquired_from_info + """ 
                                              END AS acquired_from 

                                          FROM
                                              customer_unique_email_data FULL JOIN product_issuance_data ON customer_unique_email_data.contact_email = product_issuance_data.email
                                          """)