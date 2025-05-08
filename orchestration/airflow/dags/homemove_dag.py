from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from constant_class import Constants
from generate_fake_data import IngestData

const = Constants()
ingestion = IngestData()

def fetch_customers():
    return ingestion.fetch_customers()

def fetch_properties():
    return ingestion.fetch_properties()

def fetch_transactions():
    return ingestion.fetch_transactions()

def fetch_cust_feedback():
    return ingestion.fetch_cust_feedback()

default_args = {
    'owner': 'chuks-chuks',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='homemove-etl-pipeline',
    default_args=default_args,
    description='ETL Pipeline for Homemove Project',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Fetch the raw data 
    get_raw_data = PythonOperator(
        'task_id' = 'fetch_data',
        'bash_command' = f'spark-submit {const.ABSOLUTE_PATH}/scripts/generate_fake_data.py'
    )