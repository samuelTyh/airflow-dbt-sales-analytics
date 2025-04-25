from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import os
import sys

# Register the path for docker-compose volume
sys.path.append('/opt/airflow/project')

try:
    from data_ingestion import ingest_main
    from data_ingestion import transform_main
except ImportError as e:
    print(f"Error importing modules: {e}")


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 14),
}

# Create the DAG
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sales'],
)

# Define the path to the input CSV file
csv_file_path = '/opt/airflow/data/generated_sales_data.csv'

# Task 1: Check if the CSV file exists
check_file_exists = FileSensor(
    task_id='check_file_exists',
    filepath=csv_file_path,
    fs_conn_id='fs_default',
    poke_interval=60,  # Check every 60 seconds
    timeout=300,  # Timeout after 5 minutes
    mode='poke',
    dag=dag,
)

# Task 2: Ingest data from CSV to raw DB layer
def ingest_data_to_raw():
    return ingest_main(csv_file_path)

ingest_raw_data = PythonOperator(
    task_id='ingest_raw_data',
    python_callable=ingest_data_to_raw,
    dag=dag,
)

# Task 3: Transform data from raw to staging layer
transform_raw_data = PythonOperator(
    task_id='transform_raw_data',
    python_callable=transform_main,
    dag=dag,
)

# Task 4: Archive processed file (move to processed folder)
archive_file = BashOperator(
    task_id='archive_file',
    bash_command=f'mkdir -p /opt/airflow/data/processed && cp {csv_file_path} /opt/airflow/data/processed/$(date +%Y%m%d)_sales_data.csv',
    dag=dag,
)

# Define task dependencies
check_file_exists >> ingest_raw_data >> transform_raw_data >> archive_file
