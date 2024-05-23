import os
import shutil

def copy_files_to_actividades():
    source_path = '/opt/airflow/transformacion_csv_a_xlsx'
    destination_path = '/opt/airflow/csv_actividades'
    
    # Asegúrate de que la carpeta de destino exista
    os.makedirs(destination_path, exist_ok=True)
    
    for file in os.listdir(source_path):
        if file.endswith('.csv') and 'ACTIVIDADES' in file:
            shutil.copy(os.path.join(source_path, file), destination_path)

def copy_files_to_inspecciones():
    source_path = '/opt/airflow/transformacion_csv_a_xlsx'
    destination_path = '/opt/airflow/csv_inspecciones'
    
    # Asegúrate de que la carpeta de destino exista
    os.makedirs(destination_path, exist_ok=True)
    
    for file in os.listdir(source_path):
        if file.endswith('.csv') and 'INSPECCIONES' in file:
            shutil.copy(os.path.join(source_path, file), destination_path)
