import logging
from pathlib import Path
from constant_class import Constants

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
const = Constants()


# Creating a spark session
spark = const.start_spark(title_of_app='HomeMove Raw to Bronze')
BRONZE_PATH = const.BRONZE_PATH

Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True) 

# Reading CSVs
customer_df = spark.read.option('header', 'true').csv(f"{const.RAW_PATH}/customers.csv", inferSchema=True)
properties_df = spark.read.option('header', 'true').csv(f"{const.RAW_PATH}/properties.csv", inferSchema=True)
transactions_df = spark.read.option('header', 'true').csv(f"{const.RAW_PATH}/transactions.csv", inferSchema=True)
csat_df = spark.read.option('header', 'true').csv(f"{const.RAW_PATH}/csat_surveys.csv", inferSchema=True)

# COnverting the CSV files to parquet to enable faster processing
try:
    logging.info("Now reading files")
    customer_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/customers')
    properties_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/properties')
    transactions_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/transactions')
    csat_df.write.mode('overwrite').parquet(f'{BRONZE_PATH}/csat')
except Exception as e:
    logging.error(f"Failed to convert files: {str(e)}")

spark.stop()

logging.info("Transformation completed to parquet and loaded in the bronze folder.")