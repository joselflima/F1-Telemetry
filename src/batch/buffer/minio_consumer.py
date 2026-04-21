import os
import json
import time
import logging
from datetime import datetime
import io
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

# ------------------------------
# Configurações
# ------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-telemetry")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "minio_saver_group_v3")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "telemetry")
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "10"))
FLUSH_MAX_MESSAGES = int(os.getenv("FLUSH_MAX_MESSAGES", "50"))

# ------------------------------
# Logger
# ------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def connect_kafka():
    """Tenta conectar ao Kafka com retry."""
    while True:
        try:
            logging.info(f"Conectando ao Kafka em {KAFKA_BROKER}...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Conectado ao Kafka!")
            return consumer
        except Exception as e:
            logging.error(f"Erro ao conectar no Kafka: {e}. Tentando novamente em 5s...")
            time.sleep(5)

def connect_minio():
    """Tenta conectar ao MinIO com retry."""
    while True:
        try:
            logging.info(f"Conectando ao MinIO em {MINIO_ENDPOINT}...")
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            if not client.bucket_exists(MINIO_BUCKET):
                client.make_bucket(MINIO_BUCKET)
                logging.info(f"Bucket '{MINIO_BUCKET}' criado!")
            else:
                logging.info(f"Bucket '{MINIO_BUCKET}' já existe.")
            return client
        except Exception as e:
            logging.error(f"Erro ao conectar no MinIO: {e}. Tentando novamente em 5s...")
            time.sleep(5)

def flush_buffer(minio_client, buffer):
    if not buffer:
        return
    
    # Converte a lista de dicionários para JSON
    data_str = json.dumps(buffer, ensure_ascii=False)  
    data_bytes = data_str.encode("utf-8")
    data_stream = io.BytesIO(data_bytes)
    
    # Nome do arquivo (pode ser dinâmico por timestamp)
    object_name = f"telemetry_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    
    minio_client.put_object(
        "telemetry",       # bucket
        object_name,       # nome do arquivo no MinIO
        data_stream,
        len(data_bytes),
        content_type="application/json"
    )
    buffer.clear()

# ------------------------------
# Loop principal
# ------------------------------
if __name__ == "__main__":
    consumer = connect_kafka()
    minio_client = connect_minio()
    buffer = []
    last_flush = time.time()

    logging.info("Iniciando consumo das mensagens...")
    for message in consumer:
        buffer.append(message.value)
        logging.info(f"Mensagem recebida (offset {message.offset})")

        # Condição 1: tempo desde último flush
        if time.time() - last_flush >= FLUSH_INTERVAL:
            logging.info("Flush por intervalo de tempo")
            flush_buffer(minio_client, buffer)
            last_flush = time.time()

        # Condição 2: quantidade acumulada
        if len(buffer) >= FLUSH_MAX_MESSAGES:
            logging.info("Flush por quantidade de mensagens")
            flush_buffer(minio_client, buffer)
            last_flush = time.time()
