import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts import constant_class as c

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


const = c.Constants()

default_args = {
    'owner': 'chuks-chuks', 
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='bronze layer pipeline',
    default_args=default_args,
    description='convert raw cvs to parquet files using PySpark',
    start_date=datetime(2024, 1, 1),
    schudule_interval='@daily',
    catchup=False
) as dag:
    raw_to_bronze = BashOperator(
        task_id='Transform CSVs to parquet files',
        bash_command='spark-submit /Users/phili/etl_project/homemove-analytics/scripts/process_raw_to_parquet.py'
    )

logging.info("DAG initialised and transformation to bronze completed")