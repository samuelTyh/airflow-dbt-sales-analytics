from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils.data_check import create_data_check_task

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

# Path to the CSV file for ingestion if needed
CSV_FILE_PATH = '/opt/airflow/data/generated_sales_data.csv'

# Set paths for dbt
DBT_PROJECT_DIR = '/opt/airflow/project/dbt_transform'
DBT_PROFILES_DIR = '/opt/airflow/dbt_profiles'
DBT_TARGET = 'airflow'  # Use the airflow target in the combined profiles

# Create the DAG
dag = DAG(
    'dbt_transform_pipeline',
    default_args=default_args,
    description='DBT transformation pipeline for dimensional modeling',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sales', 'dbt', 'transform'],
)

# Define dbt commands
dbt_deps_cmd = f"""
cd {DBT_PROJECT_DIR} && 
dbt deps --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""

dbt_run_staging_cmd = f"""
cd {DBT_PROJECT_DIR} && 
dbt run --models "staging.*" --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""

dbt_run_marts_cmd = f"""
cd {DBT_PROJECT_DIR} && 
dbt run --models "marts.*" --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""

dbt_test_cmd = f"""
cd {DBT_PROJECT_DIR} && 
dbt test --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""

# Create a task that checks for data and ingests if necessary
check_and_ingest_data = create_data_check_task(
    dag=dag, 
    csv_file_path=CSV_FILE_PATH,
    task_id="check_and_ingest_data",
    conn_id="sales_db"
)

# Define the dbt tasks
install_dependencies = BashOperator(
    task_id='install_dbt_dependencies',
    bash_command=dbt_deps_cmd,
    dag=dag,
)

run_staging_models = BashOperator(
    task_id='run_staging_models',
    bash_command=dbt_run_staging_cmd,
    dag=dag,
)

run_mart_models = BashOperator(
    task_id='run_mart_models',
    bash_command=dbt_run_marts_cmd,
    dag=dag,
)

test_models = BashOperator(
    task_id='test_models',
    bash_command=dbt_test_cmd,
    dag=dag,
)

# Define task dependencies
check_and_ingest_data >> install_dependencies >> run_staging_models >> run_mart_models >> test_models
