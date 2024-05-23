from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_copying import copy_files_to_actividades, copy_files_to_inspecciones

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
    'copy_csv_files',
    default_args=default_args,
    description='A DAG to copy CSV files to the appropriate folders',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='copy_to_actividades',
    python_callable=copy_files_to_actividades,
    dag=dag,
)

t2 = PythonOperator(
    task_id='copy_to_inspecciones',
    python_callable=copy_files_to_inspecciones,
    dag=dag,
)

t1 >> t2
