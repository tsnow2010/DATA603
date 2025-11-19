"""
Script:        data_cleaning.py
Authors:       Tyler Snow
Created:       2025-11-19
Last Modified: 2025-11-19
Version:       1.0

Purpose:       Provides data cleaning functionality after data is initially extracted and preprocessed from Enron dataset.
Usage:         main.ipynb

Inputs:        preproc_df: Spark DataFrame
Outputs:       /clean_body_emails/{yr}/*.csv
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

# Natural Language Tool Kit (NLTK)
import nltk
from nltk.corpus import stopwords, names
from nltk.sentiment import SentimentIntensityAnalyzer # VADER

# Constants
MONTHS = ['january','february','march','april','may','june','july','august','september','october','november','december']

# Data Cleaning

def get_cleaned_body_df(preproc_df):

    # Remove punctuation from body
    no_punct_df = preproc_df.withColumn("cleaned_body", regexp_replace("body", r"[^a-zA-Z\s]", ""))
    
    # Lower case body and extract date only
    lower_df = no_punct_df.withColumn("cleaned_body", lower(col("cleaned_body")))\
    .withColumn("date", to_date("date_time"))\
    .withColumn("year", year("date_time"))
    
    # Names
    m_names = names.words('male.txt') # English male names
    f_names = names.words('female.txt') # English female names
    all_names = [name.lower() for name in list(set(m_names + f_names))] # All names combined, lowercase
    
    # Combine all stopwords: names, articles, newline char
    
    combined_stopwords = list(stopwords.words('english')) + ['\n', 'the', '[IMAGE]'] + MONTHS + all_names 

    # UDF that removes stopwords
    def remove_stopwords(body):
        if body is None:
            return ""
        removed1 = [token for token in body.split(' ') if token not in combined_stopwords] # removes combined stopwords
        removed2 = [token.replace('\n', ' ').strip() for token in removed1] # removes newline char
        removed3 = [token for token in removed2 if len(token)>2] # remove words that are two letters or less
        return " ".join(removed3)
    
    # Remove stopwords from body
    removed_text_udf = udf(remove_stopwords, StringType())
    
    remove_stopw_df = lower_df.withColumn("cleaned_body", removed_text_udf(col("cleaned_body")))
    
    return remove_stopw_df

# Filters email datasets for only years 1999-2001, removes stopwords in emails, and writes CSV files with clean body emails
def write_clean_emails(df):

    # Remove stopwords from original emails.
    remove_stopw_df = get_cleaned_body_df(df)
    
    # Close DataFrame
    df.unpersist()
    
    # Select needed features from preprocessed emails.
    remove_stopw_df = remove_stopw_df.select("date", "year", 'sender', 'recipient', "cleaned_body")
    
    # Number of years covered by dataset
    print("Years covered in original dataset:", remove_stopw_df.select("year").distinct().orderBy("year").collect()) # List of years covered in emails. 
    
    # Select only years 1999-2001
    drop_years_df = remove_stopw_df.filter((col('year') >= 1999) & (col('year') <= 2001)) 
    remove_stopw_df.unpersist()
    
    # Create years
    years = drop_years_df.select("year").distinct().orderBy("year").collect() # List of years covered in emails.  There are odd years, e.g. 2044.
    print("Years covered in filtered dataset:", years)
    
    # EDA
    drop_years_df.show(5)
    
    # Writes CSV files for tokens by date by year for subsequent analysis
    for row in years:
        yr = row["year"]
        yearly_df = drop_years_df.filter(drop_years_df["year"] == yr).select('date','cleaned_body')
    
        # Write to disk
        yearly_df.write.csv(f"clean_body_emails/{yr}", header=True, mode='overwrite')
        print(f"Sample from {yr}:\n")
        yearly_df.show(5)
        yearly_df.unpersist()