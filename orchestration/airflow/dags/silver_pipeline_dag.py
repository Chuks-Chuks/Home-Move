import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



default_args = {
    'owner': 'chuks-chuks', 
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
}

with DAG(
    dag_id='silver_layer_pipeline',
    default_args=default_args,
    description='transform parquet files to enrich data using PySpark',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_transform_to_silver = BashOperator(
        task_id='perform-joins-to-enrich-data',
        bash_command='spark-submit /opt/airflow/scripts/bronze-to-silver/transform_bronze_to_silver.py'
    )