import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
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
RAW_PATH = Path(const.RAW_PATH)

def fetch_customers(**kwargs) -> str:
    try:
        df = ingestion.fetch_customers()
        kwargs['ti'].xcom_push(key='customers', value=True)
        return f"Customers data fetched successfully: {len(df)}"
    except Exception as e:
        logging.error(f"Failed to fetch customers: {str(e)}")
        raise


def fetch_properties(**kwargs) -> str:
    try:
        df = ingestion.fetch_properties()
        kwargs['ti'].xcom_push(key='properties', value=True)
        return f"Properties fetched successfully: {len(df)}"
    except Exception as e:
        logging.error(f"Failed to fetch properties: {str(e)}")
        raise


def fetch_transactions(**kwargs):
    try:
        ti = kwargs['ti']
        if not ti.xcom_pull(task_ids='fetch_customers', key='customers'):
            raise ValueError("Customer data not available")
        if not ti.xcom_pull(task_ids='fetch_properties', key='properties'):
            raise ValueError("Properties data not available")
        
    
        customers = pd.read_csv(RAW_PATH  / "customers.csv")
        properties = pd.read_csv(RAW_PATH / "properties.csv")
   
        ingestion.customers = customers.to_dict('records')
        ingestion.properties = properties.to_dict('records')
        df = ingestion.fetch_transactions()
        df.to_csv(RAW_PATH/ "transactions.csv", index=False)
        
        ti.xcom_push(key='transactions', value=True)
        return "transactions fetched successfully"
    except Exception as e:
        logging.error(f"Failed to fetch transactions: {str(e)}")
        raise

def fetch_cust_feedback(**kwargs):
    try:
        ti = kwargs['ti']
        if not ti.xcom_pull(task_ids='fetch_transactions', key='transactions'):
            raise ValueError("Transactions data does not exist")
        
        transactions = pd.read_csv(RAW_PATH / "transactions.csv")

        ingestion.transactions = transactions.to_dict('records')
        df = ingestion.fetch_cust_feedback()
        return "feedback fetched successfully"
    except Exception as e:
        logging.error(f"Failed to fetch feedback: {str(e)}")
        raise

default_args = {
    'owner': 'chuks-chuks',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}

with DAG(
    dag_id='homemove-etl-pipeline',
    default_args=default_args,
    description='ETL Pipeline for Homemove Project',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'homemove']
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

# Defining dependencies
task_get_customers >> task_get_properties >> task_fetch_transactions >> task_fetch_cust_feedback

logging.info("Raw data initialised and completed")