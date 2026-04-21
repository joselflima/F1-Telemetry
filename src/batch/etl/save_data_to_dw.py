"""
Script to extract, transform and load data from lake to warehouse
"""
import os
import json
import psycopg2
from minio import Minio
from minio.commonconfig import CopySource
from io import BytesIO

# ===== Configurações =====
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
SOURCE_BUCKET = "telemetry"
PROCESSED_BUCKET = "telemetry-processed"

PG_HOST = os.getenv('PG_HOST')
PG_DB = os.getenv('PG_DB')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# ===== Conexões =====
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

pg_conn = psycopg2.connect(
    host=PG_HOST,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
pg_cursor = pg_conn.cursor()

# ===== Função para inserir dados =====
def insert_telemetry(data):
    """
    Insert data into telemetry table in warehouse
    """
    query = """
    INSERT INTO telemetry (
        car_index, speed, throttle, steer, brake, clutch, gear,
        engine_rpm, drs, rev_lights_percent, rev_lights_bit_value,
        engine_temperature, brakes_temperature, tyres_surface_temperature,
        tyres_inner_temperature, tyres_pressure, surface_type
    ) VALUES (
        %(car_index)s, %(speed)s, %(throttle)s, %(steer)s, %(brake)s, %(clutch)s, %(gear)s,
        %(engine_rpm)s, %(drs)s, %(rev_lights_percent)s, %(rev_lights_bit_value)s,
        %(engine_temperature)s, %(brakes_temperature)s, %(tyres_surface_temperature)s,
        %(tyres_inner_temperature)s, %(tyres_pressure)s, %(surface_type)s
    );
    """
    pg_cursor.execute(query, data)

# ===== Processamento =====
objects = minio_client.list_objects(SOURCE_BUCKET)

for obj in objects:
    print(f"Processando arquivo: {obj.object_name}")

    # Baixar arquivo
    response = minio_client.get_object(SOURCE_BUCKET, obj.object_name)
    file_data = response.read()
    response.close()
    response.release_conn()

    # Ler JSON
    telemetry_data = json.loads(file_data.decode("utf-8"))

    # Inserir cada registro no PostgreSQL
    for record in telemetry_data:
        insert_telemetry(record)
    pg_conn.commit()

    # Mover para o bucket de processados
    minio_client.copy_object(
        PROCESSED_BUCKET,
        obj.object_name,
        CopySource(SOURCE_BUCKET, obj.object_name)
    )
    minio_client.remove_object(SOURCE_BUCKET, obj.object_name)
    print(f"Arquivo {obj.object_name} movido para {PROCESSED_BUCKET}")

pg_cursor.close()
pg_conn.close()
print("ETL concluído!")
