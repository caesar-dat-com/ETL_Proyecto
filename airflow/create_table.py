import psycopg2

# Datos de conexión
host = "localhost"
dbname = "DSfinalproject"
user = "postgres"
password = "PUTOelquelolea2024"
port = "5432"

# Conexión a la base de datos
try:
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )
    print("Conexión exitosa a la base de datos")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
    exit(1)

# Crear un cursor para ejecutar comandos
cur = conn.cursor()

# Crear la tabla
try:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS airflow_example (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("Tabla creada exitosamente")
except Exception as e:
    print(f"Error al crear la tabla: {e}")
finally:
    cur.close()
    conn.close()
