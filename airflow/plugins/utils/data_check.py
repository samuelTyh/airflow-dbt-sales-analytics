from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import PythonOperator
import sys


sys.path.append('/opt/airflow/project')

def import_ingest_module():
    try:
        from data_ingestion import ingest_main
        return ingest_main
    except ImportError as e:
        raise ImportError(f"Error importing ingest_main: {e}")


class RawDataSensor(BaseSensorOperator):
    """
    Sensor to check if there's data in the raw.sales table.
    """
    @apply_defaults
    def __init__(self, conn_id="sales_db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = "SELECT COUNT(*) FROM raw.sales"
        count = hook.get_first(sql)[0]
        self.log.info(f"Found {count} rows in raw.sales table")
        return count > 0


def check_and_ingest_data(csv_file_path, conn_id="sales_db", **context):
    """
    Check if data exists in raw.sales and ingest if empty.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    # Check if data exists
    sql = "SELECT COUNT(*) FROM raw.sales"
    count = hook.get_first(sql)[0]
    
    # If data exists, return True
    if count > 0:
        context['ti'].xcom_push(key='data_already_exists', value=True)
        return True
    
    # If no data, perform ingestion
    try:
        ingest_main = import_ingest_module()
        records_loaded = ingest_main(csv_file_path)
        
        # Verify ingestion was successful
        if records_loaded > 0:
            context['ti'].xcom_push(key='records_loaded', value=records_loaded)
            return True
        else:
            context['ti'].xcom_push(key='ingest_failed', value=True)
            return False
    except Exception as e:
        context['ti'].xcom_push(key='ingest_error', value=str(e))
        raise


def create_data_check_task(dag, csv_file_path, task_id="check_and_ingest_data", conn_id="sales_db"):
    """
    Create a task that checks for data and performs ingestion if needed.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=check_and_ingest_data,
        op_kwargs={
            'csv_file_path': csv_file_path,
            'conn_id': conn_id
        },
        dag=dag,
    )
