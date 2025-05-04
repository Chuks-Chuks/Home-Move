from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as psq



class Constants:
    def __init__(self):
        self.BRONZE_PATH = './data/bronze'
        self.SILVER_PATH = './data/silver'       
        self.GOLD_PATH = './data/gold'
        self.psq = psq

    
    def start_spark(self, title_of_app: str) -> SparkSession: 
        return SparkSession.builder.appName(title_of_app).getOrCreate()
    