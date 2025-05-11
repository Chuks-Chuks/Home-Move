import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from scripts.constant_class import Constants
    import scripts.generate_fake_data as gfd
except ImportError as ie:
    logging.warning("Import failed! Now trying alternative solution")
    import sys
    sys.path.insert(0, '/opt/airflow')
    from scripts.constant_class import Constants
    from scripts import generate_fake_data as gfd


const = Constants()
ingestion = gfd.IngestData()

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

    # Fetch the customers
    task_get_customers = PythonOperator(
        task_id='fetch_customers',
        python_callable=fetch_customers,
        dag=dag
    )

    # Fetch the properties
    task_get_properties = PythonOperator(
        task_id='fetch_properties',
        python_callable=fetch_properties,
        dag=dag
    )

    # Fetch the transactions
    task_fetch_transactions = PythonOperator(
        task_id='fetch_transactions',
        python_callable=fetch_transactions,
        dag=dag
    )

    # Fetch the customer satisfaction
    task_fetch_cust_feedback = PythonOperator(
        task_id='fetch_cust_feedback',
        python_callable=fetch_cust_feedback,
        dag=dag
    )

logging.info("Now tracking dependencies...")

# Defining dependencies
task_get_customers >> task_get_properties >> task_fetch_transactions >> task_fetch_cust_feedback

logging.info("Raw data initialised and completed")