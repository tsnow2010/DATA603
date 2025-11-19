"""
Script:        data_extraction.py
Authors:       Tyler Snow
Created:       2025-11-19
Last Modified: 2025-11-19
Version:       1.0

Purpose:       Provides data extraction functionality, parsing elements of email metadata from Enron dataset.
Usage:         main.ipynb

Inputs:        spark session, input_path (Enron dataset)
Outputs:       preproc_df: Spark DataFrame
"""

import os
import re
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import glob

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, DateType
import pyspark.pandas as ps
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, RegexTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, regexp_replace, date_format, lower, desc, lag, to_date, explode, count, udf, hour, year

# Constants
MONTHS = ['january','february','march','april','may','june','july','august','september','october','november','december']
NO_KW_LIST = ['the', 'one', 'to', 'from']

# Data Extraction

# Creates PySpark Session 
def create_spark_session(pipeline:str):
    spark_session = SparkSession.builder.appName(pipeline)\
    .config("spark.sql.ansi.enabled", "false")\
    .config("spark.driver.memory", "8g")\
    .config("spark.executor.memory", "8g")\
    .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")\
    .config("spark.sql.files.ignoreHiddenFiles", "true")\
    .getOrCreate()

    
    return spark_session

# UDF gets last, first, middle names
def get_first_last_names(email):

    # Capitalize name and remove '<'
    def capitalize_strip(name):
        return name.capitalize().strip('<')
        
    if email is None:
        return ""
    match = re.match(r'^([^@]+)@', email)
    if not match:
        return ""
    # Extract local part before @
    username = match.group(1)
    # Split on '.' and reverse order
    parts = username.split('.')
    if len(parts) == 2:
        first, last = parts
        return f"{capitalize_strip(last)}, {capitalize_strip(first)}"
    if len(parts) == 3:
        first, middle, last = parts
        return f"{capitalize_strip(last)}, {capitalize_strip(first)}, {capitalize_strip(middle)}"
    return username

# Extracts data from Enron dataset, parses relevant sections from email content, and creates .csv files for loading.
def extract_emails(spark_session, input_path):

    # Step 3: Load the dataset 
    df = spark_session.read.option("header", True) \
                   .option("multiLine", True) \
                   .option("escape", "\"") \
                   .csv(input_path)

    print("Dataset loaded successfully with multiline support.")
    df.printSchema()
    df.show(1, truncate=False)

    # Step 4: Replace literal '\n' characters with actual newlines
    df = df.withColumn("message", regexp_replace(col("message"), r"\\n", "\n"))

    # Step 5: Extract line components of emails, e.g. from, to, etc.
    from_pattern = r"From:\s*(.*)"
    to_pattern = r"To:\s*(.*)"
    date_pattern = r"Date:\s*(.*)"
    subject_pattern = r"Subject:\s*(.*)"
    body_pattern = r"(?s)\n\n(.*)"  # captures everything after headers

    cleaned_df = (
        df.withColumn("sender_email", regexp_extract(col("message"), from_pattern, 1))
          .withColumn("recipient_email", regexp_extract(col("message"), to_pattern, 1))
          .withColumn("date_raw", regexp_extract(col("message"), date_pattern, 1))
          .withColumn("subject", regexp_extract(col("message"), subject_pattern, 1))
          .withColumn("body", regexp_extract(col("message"), body_pattern, 1))
    )
    df.unpersist()

    # Step 6: Convert date strings into proper timestamps
    # Removes things like "(PDT)" that can confuse Spark's parser
    cleaned_df = cleaned_df.withColumn(
        "date_clean",
        regexp_replace(col("date_raw"), r"\(.*\)", "")
    )

    cleaned_df = cleaned_df.withColumn(
        "date_time",
        to_timestamp(col("date_clean"), "EEE, dd MMM yyyy HH:mm:ss Z")
    )

    # Step 6: Remove empty or duplicate entries
    cleaned_df = cleaned_df.na.drop(subset=["sender_email", "recipient_email", "body"])
    cleaned_df = cleaned_df.dropDuplicates(["file"])

    # Step 7: Extract sender/recipient names
    get_first_last_names_udf = udf(get_first_last_names, StringType())
    cleaned_df = cleaned_df.withColumn("sender", get_first_last_names_udf(col("sender_email")))
    cleaned_df = cleaned_df.withColumn("recipient", get_first_last_names_udf(col("recipient_email")))

    # Step 8: Select only useful columns
    final_df = cleaned_df.select("file", "sender", "recipient", "date_time", "subject", "body")

    # Step 9: Preview final preprocessed dataset
    print("Preview of Preprocessed Data:")
    final_df.show(1, truncate=False)

    # Step 10: Save preprocessed dataset
    final_df.write.csv(f"extracted_emails", header=True, mode='overwrite')

    # Step 11: Save preprocessed dataset for Sentiment Analysis (Part III)
    partIII_df = final_df.select('date_time','sender', 'recipient', 'body')
    partIII_df = partIII_df.withColumn("month", date_format(col("date_time"), "yyyy-MM"))
    partIII_df = partIII_df.select('month','sender', 'recipient', 'body')
    partIII_df.write.csv(f"month_body_emails", header=True, mode='overwrite')
    
    partIII_df.show(10)
    partIII_df.unpersist()
    return final_df
