import pandas as pd
from kafka import KafkaProducer
import json

def send_inspections_to_kafka():
    path = '/opt/airflow/csv_inspecciones/unified_inspections_file.csv'
    topic_name = 'unified_inspections'
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    df = pd.read_csv(path)
    for _, row in df.iterrows():
        producer.send(topic_name, row.to_dict())
    producer.flush()
