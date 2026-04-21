import json
import logging
import os
import signal
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from kafka import KafkaConsumer as RawKafkaConsumer
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


logging.basicConfig(
    level=logging.INFO,
    format="[Telemetry] %(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("stream")


class InfluxDBHandler:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=5000)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_point(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, Any],
        ts: Optional[datetime] = None,
    ) -> None:
        point = Point(measurement)

        for key, value in tags.items():
            point = point.tag(key, value)

        for key, value in fields.items():
            point = point.field(key, value)

        point = point.time(ts or datetime.now(timezone.utc), WritePrecision.NS)

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except Exception as exc:
            raise RuntimeError(f"falha ao escrever ponto no InfluxDB: {exc}") from exc

    def close(self) -> None:
        self.client.close()


class KafkaConsumer:
    def __init__(self, brokers: list[str], topic: str, group_id: str) -> None:
        self.consumer = RawKafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            fetch_min_bytes=10_000,
            max_partition_fetch_bytes=10_000_000,
            consumer_timeout_ms=1000,
        )
        LOGGER.info("Consumidor Kafka inicializado no topico '%s'", topic)

    def fetch_message(self, stop_event: threading.Event):
        while not stop_event.is_set():
            records = self.consumer.poll(timeout_ms=1000, max_records=1)
            if not records:
                continue

            for _, msgs in records.items():
                if msgs:
                    return msgs[0]

        return None

    def commit_message(self, msg) -> None:
        tp = TopicPartition(msg.topic, msg.partition)
        offsets = {tp: OffsetAndMetadata(msg.offset + 1, None, -1)}
        self.consumer.commit(offsets=offsets)

    def close(self) -> None:
        self.consumer.close()


def _get_first_key(payload: Dict[str, Any], keys: Tuple[str, ...], default: Any) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return default


def _array_value(values: Any, idx: int, default: Any) -> Any:
    if isinstance(values, (list, tuple)) and len(values) > idx:
        return values[idx]
    return default


def build_influx_data(payload: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    car_index = _get_first_key(payload, ("car_index", "carIndex"), 0)
    tyres_pressure = _get_first_key(payload, ("tyres_pressure", "tyresPressure"), [])
    tyres_surface_temp = _get_first_key(
        payload,
        ("tyres_surface_temperature", "tyresSurfaceTemperature"),
        [],
    )

    tags = {
        "car_index": str(int(car_index)),
    }

    fields = {
        "speed": _get_first_key(payload, ("speed",), 0),
        "throttle": float(_get_first_key(payload, ("throttle",), 0.0)),
        "steer": float(_get_first_key(payload, ("steer",), 0.0)),
        "brake": float(_get_first_key(payload, ("brake",), 0.0)),
        "gear": _get_first_key(payload, ("gear",), 0),
        "engine_rpm": _get_first_key(payload, ("engine_rpm", "rpm"), 0),
        "drs": _get_first_key(payload, ("drs",), 0),
        "tyres_pressure_RL": float(_array_value(tyres_pressure, 0, 0.0)),
        "tyres_pressure_RR": float(_array_value(tyres_pressure, 1, 0.0)),
        "tyres_pressure_FL": float(_array_value(tyres_pressure, 2, 0.0)),
        "tyres_pressure_FR": float(_array_value(tyres_pressure, 3, 0.0)),
        "tyres_surface_temp_RL": _array_value(tyres_surface_temp, 0, 0),
        "tyres_surface_temp_RR": _array_value(tyres_surface_temp, 1, 0),
        "tyres_surface_temp_FL": _array_value(tyres_surface_temp, 2, 0),
        "tyres_surface_temp_FR": _array_value(tyres_surface_temp, 3, 0),
    }

    return tags, fields


def get_env(key: str, fallback: str) -> str:
    value = os.getenv(key)
    return value if value else fallback


def main() -> None:
    LOGGER.info("Iniciando servico stream (Kafka -> InfluxDB)...")

    kafka_broker = get_env("KAFKA_BROKER", "kafka:9092")
    kafka_topic = get_env("KAFKA_TOPIC", "f1-telemetry")
    kafka_group_id = get_env("KAFKA_GROUP_ID", "influxdb-writer-group")

    influx_url = get_env("INFLUXDB_URL", "http://influxdb:8086")
    influx_token = get_env("INFLUXDB_TOKEN", "meu-token-secreto")
    influx_org = get_env("INFLUXDB_ORG", "f1-org")
    influx_bucket = get_env("INFLUXDB_BUCKET", "f1-bucket")

    stop_event = threading.Event()

    def _shutdown_handler(signum, _frame):
        LOGGER.info("Sinal %s recebido. Encerrando stream...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    influx = InfluxDBHandler(
        url=influx_url,
        token=influx_token,
        org=influx_org,
        bucket=influx_bucket,
    )
    consumer = KafkaConsumer(
        brokers=[kafka_broker],
        topic=kafka_topic,
        group_id=kafka_group_id,
    )

    try:
        while not stop_event.is_set():
            msg = consumer.fetch_message(stop_event)
            if msg is None:
                continue

            try:
                payload = json.loads(msg.value.decode("utf-8"))
            except Exception as exc:
                LOGGER.error("JSON invalido no offset %s: %s", msg.offset, exc)
                try:
                    consumer.commit_message(msg)
                except Exception as commit_exc:
                    LOGGER.error(
                        "Falha ao commitar mensagem invalida no offset %s: %s",
                        msg.offset,
                        commit_exc,
                    )
                continue

            tags, fields = build_influx_data(payload)

            try:
                influx.write_point(
                    measurement="car_telemetry",
                    tags=tags,
                    fields=fields,
                    ts=datetime.now(timezone.utc),
                )
            except Exception as exc:
                LOGGER.error("Erro ao salvar no InfluxDB (offset %s): %s", msg.offset, exc)
                continue

            try:
                consumer.commit_message(msg)
            except Exception as exc:
                LOGGER.error("Erro ao commitar offset %s: %s", msg.offset, exc)
                continue

            LOGGER.info("Offset %s processado e salvo no InfluxDB.", msg.offset)

    finally:
        try:
            consumer.close()
        finally:
            influx.close()
        LOGGER.info("Servico de stream encerrado.")


if __name__ == "__main__":
    main() 