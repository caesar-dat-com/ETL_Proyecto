import os
import shutil
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json

# Ruta base donde se encuentran los archivos CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Función para limpiar directorios
def clear_directories(directories):
    for directory in directories:
        dir_path = os.path.join(BASE_DIR, directory)
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Removed: {file_path}")

# Función para copiar archivos CSV basados en una palabra clave
def copy_csv_files(source_dir, target_dir, keyword):
    source_path = os.path.join(BASE_DIR, source_dir)
    target_path = os.path.join(BASE_DIR, target_dir)
    os.makedirs(target_path, exist_ok=True)
    for filename in os.listdir(source_path):
        if keyword.lower() in filename.lower() and filename.endswith('.csv'):
            shutil.copy(os.path.join(source_path, filename), os.path.join(target_path, filename))
            print(f"Copied: {filename}")

# Función para unificar archivos CSV
def unify_csv_files(source_dir, target_file):
    source_path = os.path.join(BASE_DIR, source_dir)
    target_path = os.path.join(BASE_DIR, target_file)
    all_files = [os.path.join(source_path, f) for f in os.listdir(source_path) if f.endswith('.csv')]
    combined_df = pd.concat([pd.read_csv(f) for f in all_files])
    combined_df.to_csv(target_path, index=False)
    print(f"Unified file saved to: {target_path}")

# Función para eliminar duplicados en un archivo CSV
def remove_duplicates(source_file):
    file_path = os.path.join(BASE_DIR, source_file)
    df = pd.read_csv(file_path)
    df.drop_duplicates(inplace=True)
    df.to_csv(file_path, index=False)
    print(f"Removed duplicates from: {file_path}")

# Función para eliminar filas vacías en un archivo CSV
def remove_empty_rows(source_file):
    file_path = os.path.join(BASE_DIR, source_file)
    df = pd.read_csv(file_path)
    df.dropna(inplace=True)
    df.to_csv(file_path, index=False)
    print(f"Removed empty rows from: {file_path}")

# Función para calcular la sumatoria de la columna "Duración" en cada archivo CSV
def sumar_duracion(source_file):
    file_path = os.path.join(BASE_DIR, source_file)
    try:
        df = pd.read_csv(file_path)
        df['Duración'] = pd.to_numeric(df['Duración'].str.replace(',', '.'), errors='coerce')
        suma_duracion = df['Duración'].dropna().sum()
        print(f"Total Duration for {source_file}: {suma_duracion}")
        return suma_duracion
    except Exception as e:
        print(f"Error processing {source_file}: {e}")
        return None

# Función para enviar datos a Kafka
def send_to_kafka(source_file, topic):
    file_path = os.path.join(BASE_DIR, source_file)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    with open(file_path, 'r') as file:
        data = pd.read_csv(file)
        for index, row in data.iterrows():
            message = row.to_dict()
            producer.send(topic, value=message)
            producer.flush()
        print(f"Data from {source_file} sent to Kafka topic '{topic}'")

def sumar_duracion_planeada(source_file):
    file_path = os.path.join(BASE_DIR, source_file)
    try:
        df = pd.read_csv(file_path)
        if 'Nombre' in df.columns and df['Nombre'].notna().any():
            columna_usada = 'Nombre'
        elif 'codigo_personal_text' in df.columns and df['codigo_personal_text'].notna().any():
            columna_usada = 'codigo_personal_text'
        else:
            print("Columna relevante no encontrada")
            return None
        df['Duración'] = pd.to_numeric(df['Duración'].str.replace(',', '.'), errors='coerce')
        df_filtrado = df[df[columna_usada].notna() & df[columna_usada].str.strip().astype(bool)]
        suma_duracion_planeada = df_filtrado['Duración'].dropna().sum()
        print(f"Suma de duración planeada para {source_file} usando columna {columna_usada}: {suma_duracion_planeada}")
        return suma_duracion_planeada
    except Exception as e:
        print(f"Error processing {source_file}: {e}")
        return None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'indicadores_dag',
    default_args=default_args,
    description='DAG that clears directories, copies, unifies, removes duplicates and empty rows, calculates total duration, and sends data to Kafka',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    clear_directories_task = PythonOperator(
        task_id='clear_directories',
        python_callable=clear_directories,
        op_kwargs={'directories': ['CSV_ACTIVIDAD', 'CSV_INSPECCION', 'CSV_unifi']},
    )

    copy_activities_task = PythonOperator(
        task_id='copy_activities_csv_files',
        python_callable=copy_csv_files,
        op_kwargs={'source_dir': 'CSV_DATA', 'target_dir': 'CSV_ACTIVIDAD', 'keyword': 'actividades'},
    )

    copy_inspecciones_task = PythonOperator(
        task_id='copy_inspecciones_csv_files',
        python_callable=copy_csv_files,
        op_kwargs={'source_dir': 'CSV_DATA', 'target_dir': 'CSV_INSPECCION', 'keyword': 'inspecciones'},
    )

    unify_activities_task = PythonOperator(
        task_id='unify_activities_csv_files',
        python_callable=unify_csv_files,
        op_kwargs={'source_dir': 'CSV_ACTIVIDAD', 'target_file': 'CSV_unifi/unified_activities.csv'},
    )

    unify_inspecciones_task = PythonOperator(
        task_id='unify_inspecciones_csv_files',
        python_callable=unify_csv_files,
        op_kwargs={'source_dir': 'CSV_INSPECCION', 'target_file': 'CSV_unifi/unified_inspecciones.csv'},
    )

    remove_duplicates_activities_task = PythonOperator(
        task_id='remove_duplicates_activities_csv_files',
        python_callable=remove_duplicates,
        op_kwargs={'source_file': 'CSV_unifi/unified_activities.csv'},
    )

    remove_duplicates_inspecciones_task = PythonOperator(
        task_id='remove_duplicates_inspecciones_csv_files',
        python_callable=remove_duplicates,
        op_kwargs={'source_file': 'CSV_unifi/unified_inspecciones.csv'},
    )

    remove_empty_rows_activities_task = PythonOperator(
        task_id='remove_empty_rows_activities_csv_files',
        python_callable=remove_empty_rows,
        op_kwargs={'source_file': 'CSV_unifi/unified_activities.csv'},
    )

    remove_empty_rows_inspecciones_task = PythonOperator(
        task_id='remove_empty_rows_inspecciones_csv_files',
        python_callable=remove_empty_rows,
        op_kwargs={'source_file': 'CSV_unifi/unified_inspecciones.csv'},
    )

    calculate_duration_activities_task = PythonOperator(
        task_id='calculate_duration_activities',
        python_callable=sumar_duracion,
        op_kwargs={'source_file': 'CSV_unifi/unified_activities.csv'},
    )

    calculate_duration_inspecciones_task = PythonOperator(
        task_id='calculate_duration_inspecciones',
        python_callable=sumar_duracion,
        op_kwargs={'source_file': 'CSV_unifi/unified_inspecciones.csv'},
    )

    calculate_duration_planeada_activities_task = PythonOperator(
    task_id='calculate_duration_planeada_activities',
    python_callable=sumar_duracion_planeada,
    op_kwargs={'source_file': 'CSV_unifi/unified_activities.csv'},
    )

    calculate_duration_planeada_inspecciones_task = PythonOperator(
    task_id='calculate_duration_planeada_inspecciones',
    python_callable=sumar_duracion_planeada,
    op_kwargs={'source_file': 'CSV_unifi/unified_inspecciones.csv'},
    )

    send_to_kafka_activities_task = PythonOperator(
        task_id='send_to_kafka_activities',
        python_callable=send_to_kafka,
        op_kwargs={'source_file': 'CSV_unifi/unified_activities.csv', 'topic': 'activities_topic'},
    )

    send_to_kafka_inspecciones_task = PythonOperator(
        task_id='send_to_kafka_inspecciones',
        python_callable=send_to_kafka,
        op_kwargs={'source_file': 'CSV_unifi/unified_inspecciones.csv', 'topic': 'inspecciones_topic'},
    )

    clear_directories_task >> [copy_activities_task, copy_inspecciones_task]
    copy_activities_task >> unify_activities_task >> remove_duplicates_activities_task >> remove_empty_rows_activities_task >> calculate_duration_activities_task >> calculate_duration_planeada_activities_task >> send_to_kafka_activities_task
    copy_inspecciones_task >> unify_inspecciones_task >> remove_duplicates_inspecciones_task >> remove_empty_rows_inspecciones_task >> calculate_duration_inspecciones_task >> calculate_duration_planeada_inspecciones_task >> send_to_kafka_inspecciones_task
