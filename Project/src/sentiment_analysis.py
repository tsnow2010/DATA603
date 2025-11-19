import os
import re
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import glob
from pathlib import Path

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

NO_KW_LIST = ['the', 'one', 'to', 'from']

# Part III: Sentiment Analysis

# Load clean_body_emails.csv into Spark Session, find keyword/sentences, determine VADER sentiment
def load_and_extract_sentences(spark_session, schema, sia, emails_by_month_dir, monthly_trends_dir):

    def get_kw_month_dict(trends_csv_file):
 
        trends_df = pd.read_csv(trends_csv_file)
    
        # Filter rows where acceleration >= 10000
        trends_df_10000 = trends_df[trends_df["acceleration"] > 10000]
        trends_df_10000 = trends_df_10000[~trends_df_10000["token"].isin(NO_KW_LIST)]
        
        kw_tuple_month_df = trends_df_10000[["month","token"]]
        return kw_tuple_month_df.groupby("month")['token'].apply(lambda x: tuple(x)).to_dict()
    
    trends_all_years = [os.path.join(monthly_trends_dir, yr) for yr in os.listdir(monthly_trends_dir)]

    for monthly_trend in trends_all_years:

        if ".DS_Store" in monthly_trend:
            continue

        print(f'Opening "{monthly_trend}" file')
    
        trends_csv_file = glob.glob(f"{monthly_trend}/part-*.csv")[0]

        df = spark_session.read.csv(emails_by_month_dir, schema=schema)

        results = []
    
        for month, tokens in get_kw_month_dict(trends_csv_file).items():
            # Filter emails by month
            emails_df = df.filter(col('month') == month)
            emails_pdf = emails_df.toPandas()
        
    
            # Loop through each row in the DataFrame
            for idx, row in emails_pdf.iterrows():
                body = str(row.get("body", ""))  # Safely handle missing or non-string values
                sender = str(row.get("sender", ""))
                recipient = str(row.get("recipient", ""))
                if not body.strip():
                    continue  # Skip empty bodies
            
                # Split body into sentences using punctuation
                sentences = re.split(r'(?<=[.!?])\s+', body)
            
                # Check each sentence for the keyword
                for sentence in sentences:
                    token_count = 0
                    found_tokens = ()
                    for token in tokens:
                        if token.lower() in sentence.lower():
                            token_count += 1
                            found_tokens += (token,)
            
                    if token_count != 0:
                        sentiment = sia.polarity_scores(sentence)["compound"]
                        results.append({
                                "month": month,
                                "tokens": found_tokens,
                                "token_count": token_count,
                                "sentence": sentence,
                                'sender': sender,
                                'recipient': recipient,
                                "sentiment": sentiment
                        })
        pd.DataFrame(results).to_csv(f'{str(Path.cwd())}/{monthly_trend[-4:]}_results.csv', index=False, header=True)
        print('Results printed!')