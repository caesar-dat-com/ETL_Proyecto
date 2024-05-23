from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_processing_inspections import unify_inspections_files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'unify_inspections_files',
    default_args=default_args,
    description='A DAG to unify inspections CSV files',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='unify_inspections_files',
    python_callable=unify_inspections_files,
    dag=dag,
)
