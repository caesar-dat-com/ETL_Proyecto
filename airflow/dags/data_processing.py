import pandas as pd
import os

def unify_csv_files():
    # Cambia la ruta a una ruta de Linux cuando se ejecuta en un entorno de Docker o WSL
    path = '/opt/airflow/csv_actividades'
    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.csv')]
    df_list = []

    # Lista para almacenar las columnas encontradas en todos los archivos
    all_columns = set()

    for file in all_files:
        # Usa error_bad_lines y warn_bad_lines para manejar archivos CSV mal formados
        df = pd.read_csv(file, error_bad_lines=False, warn_bad_lines=True)
        # Actualiza el conjunto de columnas
        all_columns.update(df.columns)
        df_list.append(df)

    # Asegúrate de que cada DataFrame tenga todas las columnas encontradas en los archivos
    df_list = [df.reindex(columns=all_columns) for df in df_list]

    # Concatena todas las DataFrames, ignorando los índices para no tener conflictos
    unified_df = pd.concat(df_list, ignore_index=True, sort=False)
    # Elimina cualquier fila que contenga sólo valores NaN
    unified_df.dropna(how='all', inplace=True)
    # Guarda el archivo unificado
    unified_df.to_csv(os.path.join(path, "unified_file.csv"), index=False)

# Llamar a la función para ejecutar la unificación
unify_csv_files()
