from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def list_files():
    # Cambia la ruta a una ruta de Linux cuando se ejecuta en un entorno de Docker o WSL
    path = '/opt/airflow/csv_actividades'
    print("Listando archivos en el directorio:", path)
    files = [f for f in os.listdir(path) if f.endswith('.csv')]
    for file in files:
        print(file)

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
    'unify_csv_files',
    default_args=default_args,
    description='A DAG to check CSV files directory',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='list_files',
    python_callable=list_files,
    dag=dag,
)
