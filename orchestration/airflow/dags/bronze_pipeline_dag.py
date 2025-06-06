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
    dag_id='bronze_layer_pipeline',
    default_args=default_args,
    description='convert raw cvs to parquet files using PySpark',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    raw_to_bronze = BashOperator(
        task_id='transform-csvs-to-parquet-files',
        bash_command='spark-submit /opt/airflow/scripts/raw-to-parquet/process_raw_to_parquet.py'
    )


logging.info("DAG initialised and transformation to bronze completed")