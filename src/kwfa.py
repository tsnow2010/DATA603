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
NO_KW_LIST = ['the', 'one', 'to', 'from']

# Part II: Keyword Frequency Acceleration

# Loads and tokenizes cleaned email bodies by year from CSV files into new Spark session
def load_and_tokenize(spark_session, schema, files):
    df = spark_session.read.csv(files, header=True, schema=schema)
    df = df.filter(df["cleaned_body"].isNotNull())
    df = df.withColumn("date", to_date(col("date")))  # ensure date format
    df = df.withColumn("month", date_format(col("date"), "yyyy-MM"))
    tokenizer = RegexTokenizer(inputCol="cleaned_body", outputCol="tokens", pattern="\\W")
    tokenized = tokenizer.transform(df).select("month", "tokens")
    return tokenized.select("month", explode("tokens").alias("token")).filter(col("token") != "") # also removes empty tokens

# Compute monthly keyword frequencies
def compute_monthly_counts(token_df):
    return token_df.groupBy("month", "token").agg(count("*").alias("freq"))

# Compute acceleration of keyword frequencies with month windows
def compute_acceleration(freq_df):
    window = Window.partitionBy("token").orderBy("month")
    freq_df = freq_df.withColumn("prev_freq", lag("freq").over(window))
    freq_df = freq_df.withColumn("acceleration", col("freq") - col("prev_freq"))
    return freq_df
    
# Finds top n accelerators (most accelerating keyword frequencies)
def get_top_accelerators(accel_df, top_n=100):
    return accel_df.orderBy(desc("acceleration")).limit(top_n)

# Defines data pipeline
def run_pipeline(spark_session, schema, base_dir, output_path):
    all_years = [os.path.join(base_dir, yr) for yr in os.listdir(base_dir)]
    
    for yr_dir in all_years:
        df = load_and_tokenize(spark_session, schema, yr_dir)
        monthly_freq = compute_monthly_counts(df)
        accel_df = compute_acceleration(monthly_freq)
        top_trends = get_top_accelerators(accel_df)
        year = os.path.basename(yr_dir)
        top_trends.write.csv(f"{output_path}/{year}", header=True, mode="overwrite")
    # all_years = [os.path.join(base_dir, yr) for yr in os.listdir(base_dir)]
    # all_tokens = [load_and_tokenize(spark_session, schema, yr_dir) for yr_dir in all_years]
    # combined = all_tokens[0]
    # for df in all_tokens[1:]:
    #     combined = combined.union(df)

    # monthly_freq = compute_monthly_counts(combined)
    # accel_df = compute_acceleration(monthly_freq)
    # top_trends = get_top_accelerators(accel_df)

    # top_trends.write.csv(f"{output_path}", header=True, mode="overwrite")
    # return top_trends