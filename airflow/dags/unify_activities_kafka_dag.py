from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_activities_processing_kafka import send_activities_to_kafka

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
    'unify_activities_kafka_dag',
    default_args=default_args,
    description='A DAG to send unified activities data to Kafka',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='send_activities_to_kafka',
    python_callable=send_activities_to_kafka,
    dag=dag,
)
