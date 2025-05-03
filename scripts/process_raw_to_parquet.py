from pyspark.sql import SparkSession
from pathlib import Path

# Creating a spark session
spark = SparkSession.builder.appName('HomeMove Raw to Bronze').getOrCreate()

RAW_PATH = './data/raw'
BRONZE_PATH = './data/bronze'

Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True) 

# Reading CSVs
customer_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/customers.csv", inferSchema=True)
properties_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/properties.csv", inferSchema=True)
transactions_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/transactions.csv", inferSchema=True)
csat_df = spark.read.option('header', 'true').csv(f"{RAW_PATH}/csat_surveys.csv", inferSchema=True)

# COnverting the CSV files to parquet to enable faster processing

customer_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/customers')
properties_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/properties')
transactions_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/transactions')
csat_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/csat')

spark.stop()