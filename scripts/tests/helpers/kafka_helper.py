"""Kafka topic polling for test validation."""

import json
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def poll_topic(credentials: dict[str, str], topic: str, min_count: int, timeout: int) -> list[dict]:  # type: ignore[type-arg]
    """Consume messages from a Kafka topic and return decoded records.

    Attempts Avro deserialization via Schema Registry; falls back to JSON,
    then raw string.

    Args:
        credentials: Dict from extract_kafka_credentials() — must include
                     bootstrap_servers, kafka_api_key, kafka_api_secret,
                     schema_registry_url, schema_registry_api_key,
                     schema_registry_api_secret
        topic: Topic name to consume from
        min_count: Stop consuming once this many messages have been collected
        timeout: Maximum seconds to consume before returning

    Returns:
        List of decoded message dicts (up to min_count + small buffer)
    """
    sr_client = SchemaRegistryClient(
        {
            "url": credentials["schema_registry_url"],
            "basic.auth.user.info": (
                f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}"
            ),
        }
    )
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer(
        {
            "bootstrap.servers": credentials["bootstrap_servers"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": credentials["kafka_api_key"],
            "sasl.password": credentials["kafka_api_secret"],
            "group.id": f"test-consumer-{topic}-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    messages: list[dict] = []  # type: ignore[type-arg]
    start = time.time()

    try:
        consumer.subscribe([topic])
        while len(messages) < min_count:
            if time.time() - start > timeout:
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw = msg.value()
            if raw is None:
                continue

            # Try Avro first, then JSON, then raw string
            record: dict = {}  # type: ignore[type-arg]
            try:
                decoded = avro_deserializer(raw, SerializationContext(topic, MessageField.VALUE))
                record = decoded if isinstance(decoded, dict) else {"value": decoded}
            except Exception:
                try:
                    record = json.loads(raw.decode("utf-8"))
                except Exception:
                    record = {"raw": raw.decode("utf-8", errors="replace")}

            messages.append(record)

    finally:
        consumer.close()

    return messages
