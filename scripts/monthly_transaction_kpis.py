from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

SILVER_PATH = './data/silver'
GOLD_PATH = './data/gold'
Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)
# Starting a Spark Session

spark = SparkSession.builder.appName('MonthlyGoldLayer').getOrCreate()
