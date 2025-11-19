# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, DateType
import pyspark.pandas as ps
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, RegexTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, regexp_replace, date_format, lower, desc, lag, to_date, explode, count, udf, hour, year

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