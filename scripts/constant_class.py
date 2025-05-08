from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as psq



class Constants:
    def __init__(self):
        self.BRONZE_PATH = './data/bronze'
        self.SILVER_PATH = './data/silver'       
        self.GOLD_PATH = './data/gold'
        self.REPORTING_PATH = './data/reporting'
        self.RAW_PATH = './data/raw'
        self.ABSOLUTE_PATH = "./Users/phili/etl_project/homemove-analytics"
        self.psq = psq

    
    def start_spark(self, title_of_app: str) -> SparkSession: 
        return SparkSession.builder.appName(title_of_app).getOrCreate()
    