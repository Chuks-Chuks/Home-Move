from pyspark.sql import SparkSession
import pyspark.sql.functions as psq

class Constants:
    def __init__(self):
        self.BASE_PATH = '/opt/airflow/'
        self.BRONZE_PATH = f'{self.BASE_PATH}/data/bronze'
        self.SILVER_PATH = f'{self.BASE_PATH}/data/silver'       
        self.GOLD_PATH = f'{self.BASE_PATH}/data/gold'
        self.REPORTING_PATH = f'{self.BASE_PATH}/data/reporting'
        self.RAW_PATH = f'{self.BASE_PATH}/data/raw'
        self.ABSOLUTE_PATH = "./Users/phili/etl_project/homemove-analytics"
        self.psq = psq
        self.NUM_CUSTOMERS = 15000
        self.NUM_PROPERTIES = 9000
        self.NUM_TRANSACTIONS = 8500
        self.NUM_CSATS = 5500
        self.MINIMUM_HOUSE_PRICE = 150000
        self.MAXIMUM_HOUSE_PRICE = 750000


    def start_spark(self, title_of_app: str) -> SparkSession: 
        return SparkSession.builder.appName(title_of_app).getOrCreate()
    