from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

spark = SparkSession.builder.appName('HomeMove Raw to Bronze').getOrCreate()

RAW_PATH = '../data/raw'
BRONZE_PATH = '../data/bronze'

Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True) 

# Reading CSVs
customer_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/customers.csv")
properties_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/properties.csv")
transactions_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/transactions.csv")
csat_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/csat_surveys.csv")

customer_df.show(5)