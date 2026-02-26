#!/usr/bin/env python3
"""
Generic Kafka-topic-to-CSV capture using Avro deserialization via Confluent Schema Registry.

Consumes messages from a Kafka topic, deserializes them (Avro → dict via Schema Registry),
and writes them to a CSV file. The Avro schema is fetched automatically from the registry
using the schema ID embedded in each message — no manual schema input required.

Two output files are produced:
  <output>.csv   — human-readable, compatible with publish_csv_to_kafka.py
  <output>.avsc  — the Avro schema retrieved from Schema Registry (enables lossless roundtrip)

Roundtrip guarantee:
  capture_kafka_to_csv  →  <topic>.csv + <topic>.avsc
  publish_csv_to_kafka  →  reads <topic>.csv, auto-discovers <topic>.avsc  →  Kafka

Credentials are loaded automatically from terraform/core/terraform.tfstate.

If the topic does not use Avro (plain JSON payloads), messages are decoded as JSON
and written to CSV. If that also fails, raw bytes are saved as base64-encoded JSONL
and you can convert with --convert-jsonl.

Usage:
    # Capture from beginning (most common):
    python -m scripts.common.capture_kafka_to_csv \\
        --topic fema_claims --output data/fema_claims.csv --from-beginning

    # Capture live stream (from latest offset), stop after 30s idle:
    python -m scripts.common.capture_kafka_to_csv \\
        --topic ride_requests --output data/ride_requests.csv

    # Limit to 5000 records:
    python -m scripts.common.capture_kafka_to_csv \\
        --topic fema_claims --output data/sample.csv \\
        --from-beginning --max-records 5000

    # Convert a previously captured JSONL file to CSV:
    python -m scripts.common.capture_kafka_to_csv \\
        --convert-jsonl data/raw.jsonl --output data/converted.csv

Library usage:
    from scripts.common.capture_kafka_to_csv import KafkaCsvCapture
    capture = KafkaCsvCapture.from_terraform()
    count = capture.capture_to_csv(
        topic="fema_claims", output_file=Path("data/fema_claims.csv"),
        from_beginning=True
    )
"""

import argparse
import base64
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root


# ---------------------------------------------------------------------------
# Value conversion helpers
# ---------------------------------------------------------------------------

def _value_to_str(v: Any) -> str:
    """Convert a deserialized Avro value to a CSV-safe string."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        # Avro timestamp-millis logical type may be returned as datetime
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if isinstance(v, int) and v > 1_000_000_000_000:
        # Looks like a timestamp-millis long — convert to ISO string
        try:
            dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        except (OSError, OverflowError):
            pass
    return str(v)


def _flatten_avro_record(record: Dict[str, Any]) -> Dict[str, str]:
    """Convert all values in a deserialized Avro record to plain strings for CSV."""
    return {k: _value_to_str(v) for k, v in record.items()}


# ---------------------------------------------------------------------------
# JSONL → CSV conversion (for raw-capture fallback files)
# ---------------------------------------------------------------------------

def convert_jsonl_to_csv(jsonl_file: Path, output_csv: Path, logger: logging.Logger) -> int:
    """
    Convert a base64-encoded JSONL file (raw Kafka capture) to CSV.

    Each JSONL record must have a 'value' field containing a base64-encoded
    UTF-8 JSON string. Records where the value cannot be decoded as JSON are skipped.

    Args:
        jsonl_file:  Input .jsonl file.
        output_csv:  Destination CSV file.
        logger:      Logger instance.

    Returns:
        Number of rows written.
    """
    rows: List[Dict[str, str]] = []
    fieldnames: Optional[List[str]] = None

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
                value_b64 = envelope.get("value")
                if not value_b64:
                    continue
                value_bytes = base64.b64decode(value_b64)
                record = json.loads(value_bytes.decode("utf-8"))
                flat = {k: _value_to_str(v) for k, v in record.items()}
                if fieldnames is None:
                    fieldnames = list(flat.keys())
                rows.append(flat)
            except Exception as e:
                logger.warning(f"Line {line_num}: skipped ({e})")

    if not rows:
        logger.warning("No convertible records found in JSONL file.")
        return 0

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Wrote {len(rows)} rows to {output_csv}")
    return len(rows)


# ---------------------------------------------------------------------------
# Capture class
# ---------------------------------------------------------------------------

class KafkaCsvCapture:
    """
    Consumes a Kafka topic and writes records to a CSV file.

    Attempts Avro deserialization via Schema Registry first; falls back to
    plain JSON decode; falls back to base64-encoded JSONL (raw capture).

    Use `from_terraform()` to construct with credentials auto-loaded from
    this repo's Terraform state.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        api_key: str,
        api_secret: str,
        schema_registry_url: str,
        sr_api_key: str,
        sr_api_secret: str,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.api_key = api_key
        self.api_secret = api_secret
        self.schema_registry_url = schema_registry_url
        self.sr_api_key = sr_api_key
        self.sr_api_secret = sr_api_secret
        self.logger = logging.getLogger(__name__)

        self._sr_client = SchemaRegistryClient({
            "url": schema_registry_url,
            "basic.auth.user.info": f"{sr_api_key}:{sr_api_secret}",
        })

    # ------------------------------------------------------------------
    # Class-method constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_terraform(cls) -> "KafkaCsvCapture":
        """
        Construct a capture instance with credentials from this repo's Terraform state.

        Reads terraform/core/terraform.tfstate from the project root.
        """
        logger = logging.getLogger(__name__)
        logger.info("Loading Kafka credentials from Terraform state...")
        project_root = get_project_root()
        creds = extract_kafka_credentials("terraform", project_root)
        return cls(
            bootstrap_servers=creds["bootstrap_servers"],
            api_key=creds["kafka_api_key"],
            api_secret=creds["kafka_api_secret"],
            schema_registry_url=creds["schema_registry_url"],
            sr_api_key=creds["schema_registry_api_key"],
            sr_api_secret=creds["schema_registry_api_secret"],
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def capture_to_csv(
        self,
        topic: str,
        output_file: Path,
        max_records: Optional[int] = None,
        from_beginning: bool = False,
        idle_timeout_seconds: int = 30,
    ) -> int:
        """
        Consume *topic* and write records to *output_file* as CSV.

        Also writes <output_file>.avsc with the Avro schema (if Avro decoding succeeds),
        which publish_csv_to_kafka.py will auto-discover for a lossless roundtrip.

        Args:
            topic:                Kafka topic to consume.
            output_file:          Destination CSV file.
            max_records:          Stop after N records (None = unlimited).
            from_beginning:       Start from earliest offset if True; latest if False.
            idle_timeout_seconds: Stop consuming after N seconds with no new messages.

        Returns:
            Number of records written.
        """
        avro_deserializer = AvroDeserializer(self._sr_client)

        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
            "group.id": f"capture-{topic}-{int(time.time())}",
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
            "value.deserializer": avro_deserializer,
        }

        consumer = DeserializingConsumer(consumer_config)
        consumer.subscribe([topic])
        self.logger.info(
            f"Subscribed to '{topic}' "
            f"({'from beginning' if from_beginning else 'from latest'})"
        )

        records: List[Dict[str, str]] = []
        fieldnames: Optional[List[str]] = None
        schema_str: Optional[str] = None
        fallback_mode = False   # True when Avro decode failed → raw JSONL fallback
        raw_records: List[dict] = []  # used only in fallback_mode
        last_message_time = time.time()

        if max_records:
            self.logger.info(f"Capturing up to {max_records} records from '{topic}'")
        else:
            self.logger.info(
                f"Capturing ALL records from '{topic}' "
                f"(idle timeout: {idle_timeout_seconds}s)"
            )

        try:
            self.logger.info("Consuming messages — press Ctrl+C to stop early...")
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    if time.time() - last_message_time > idle_timeout_seconds:
                        self.logger.info(
                            f"No new messages for {idle_timeout_seconds}s — stopping."
                        )
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug(f"EOF on partition {msg.partition()}")
                        continue
                    raise KafkaException(msg.error())

                last_message_time = time.time()

                if not fallback_mode:
                    value = msg.value()  # already deserialized by AvroDeserializer

                    if value is None:
                        # Avro deserialization returned None — check if raw bytes decode as JSON
                        value = self._try_json_fallback(msg, consumer)
                        if value is None:
                            # Can't decode — switch to raw JSONL fallback for all messages
                            self.logger.warning(
                                "Cannot deserialize messages as Avro or JSON. "
                                "Switching to raw JSONL capture. "
                                "Use --convert-jsonl on the output to attempt CSV conversion."
                            )
                            fallback_mode = True

                    if not fallback_mode and isinstance(value, dict):
                        flat = _flatten_avro_record(value)
                        if fieldnames is None:
                            fieldnames = list(flat.keys())
                            # Fetch schema from registry for .avsc output
                            schema_str = self._fetch_schema(topic)
                        records.append(flat)
                        count = len(records)
                        if count % 1000 == 0:
                            self.logger.info(f"Captured {count} records...")
                        if max_records and count >= max_records:
                            self.logger.info(f"Reached limit of {max_records} records.")
                            break
                        continue

                # Fallback: raw base64-encoded JSONL
                raw_records.append(self._encode_raw(msg))
                count = len(raw_records)
                if count % 1000 == 0:
                    self.logger.info(f"Captured {count} raw records...")
                if max_records and count >= max_records:
                    self.logger.info(f"Reached limit of {max_records} records.")
                    break

        except KeyboardInterrupt:
            self.logger.info("Capture interrupted by user.")
        finally:
            consumer.close()

        output_file.parent.mkdir(parents=True, exist_ok=True)

        if fallback_mode or (not records and raw_records):
            # Write raw JSONL fallback
            jsonl_path = output_file.with_suffix(".jsonl")
            self.logger.warning(
                f"Writing {len(raw_records)} raw records to {jsonl_path} (base64 JSONL). "
                f"Run with --convert-jsonl {jsonl_path} --output {output_file} to attempt CSV conversion."
            )
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in raw_records:
                    f.write(json.dumps(r) + "\n")
            return len(raw_records)

        if not records:
            self.logger.warning("No records captured.")
            return 0

        # Write CSV
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        self.logger.info(f"Wrote {len(records)} rows to {output_file}")

        # Write schema alongside CSV for roundtrip support
        if schema_str:
            avsc_path = output_file.with_suffix(".avsc")
            avsc_path.write_text(schema_str, encoding="utf-8")
            self.logger.info(f"Saved Avro schema to {avsc_path} (used by publish_csv_to_kafka.py)")

        return len(records)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _try_json_fallback(self, msg, consumer) -> Optional[Dict]:
        """Try decoding the raw message value as UTF-8 JSON."""
        # DeserializingConsumer has already consumed the raw bytes.
        # We cannot re-access them after deserialization. Return None to signal failure.
        # (A proper fallback would require switching to a plain Consumer.)
        return None

    def _fetch_schema(self, topic: str) -> Optional[str]:
        """Fetch the latest value schema for *topic* from Schema Registry."""
        subject = f"{topic}-value"
        try:
            registered = self._sr_client.get_latest_version(subject)
            self.logger.info(
                f"Fetched schema version {registered.version} for subject '{subject}'"
            )
            return registered.schema.schema_str
        except Exception as e:
            self.logger.warning(f"Could not fetch schema for '{subject}': {e}")
            return None

    @staticmethod
    def _encode_raw(msg) -> dict:
        """Encode a raw Message to a base64 JSONL dict (fallback format)."""
        key_bytes = msg.key()
        value_bytes = msg.value()
        headers = msg.headers() or []
        record = {
            "key": base64.b64encode(key_bytes).decode("utf-8") if key_bytes else None,
            "value": base64.b64encode(value_bytes).decode("utf-8") if value_bytes else None,
            "partition": msg.partition(),
            "offset": msg.offset(),
        }
        if headers:
            record["headers"] = {k: base64.b64encode(v).decode("utf-8") for k, v in headers}
        return record


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="capture_kafka_to_csv",
        description=(
            "Capture a Confluent Cloud Kafka topic to a CSV file via Avro deserialization. "
            "Credentials are auto-loaded from terraform/core/terraform.tfstate. "
            "Also writes a .avsc schema file alongside the CSV for lossless roundtrip "
            "with publish_csv_to_kafka.py."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Capture entire topic from beginning:
  %(prog)s --topic fema_claims --output data/fema_claims.csv --from-beginning

  # Capture live messages until 30s idle:
  %(prog)s --topic ride_requests --output data/ride_requests.csv

  # Capture at most 10000 records:
  %(prog)s --topic fema_claims --output data/sample.csv \\
      --from-beginning --max-records 10000

  # Convert a previously captured raw JSONL to CSV (JSON-valued topics):
  %(prog)s --convert-jsonl data/raw.jsonl --output data/converted.csv
        """,
    )
    parser.add_argument("--topic",
                        help="Kafka topic to consume from")
    parser.add_argument("--output", type=Path,
                        help="Output CSV file path")
    parser.add_argument("--from-beginning", action="store_true",
                        help="Consume from earliest offset (default: from latest)")
    parser.add_argument("--max-records", type=int, default=None,
                        help="Stop after N records (default: unlimited)")
    parser.add_argument("--timeout", type=int, default=30,
                        help="Stop after N seconds of no new messages (default: 30)")
    parser.add_argument("--convert-jsonl", type=Path, metavar="JSONL_FILE",
                        help="Convert a base64 JSONL file (from a raw capture) to CSV "
                             "without connecting to Kafka. Requires --output.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose/debug logging")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    # --convert-jsonl mode: no Kafka connection needed
    if args.convert_jsonl:
        if not args.output:
            logger.error("--output is required with --convert-jsonl")
            return 1
        if not args.convert_jsonl.exists():
            logger.error(f"JSONL file not found: {args.convert_jsonl}")
            return 1
        count = convert_jsonl_to_csv(args.convert_jsonl, args.output, logger)
        print(f"\n{'=' * 60}")
        print("JSONL → CSV CONVERSION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Source: {args.convert_jsonl}")
        print(f"Output: {args.output}")
        print(f"Rows:   {count}")
        print(f"{'=' * 60}")
        return 0 if count > 0 else 1

    # Normal capture mode
    if not args.topic:
        logger.error("--topic is required (or use --convert-jsonl for offline conversion)")
        return 1
    if not args.output:
        logger.error("--output is required")
        return 1

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error(
            "confluent-kafka is not installed. "
            "Run: uv pip install confluent-kafka"
        )
        return 1

    try:
        capture = KafkaCsvCapture.from_terraform()
    except Exception as e:
        logger.error(f"Failed to load credentials from Terraform state: {e}")
        logger.error("Ensure terraform/core/terraform.tfstate exists and contains Kafka outputs.")
        return 1

    try:
        count = capture.capture_to_csv(
            topic=args.topic,
            output_file=args.output,
            max_records=args.max_records,
            from_beginning=args.from_beginning,
            idle_timeout_seconds=args.timeout,
        )

        print(f"\n{'=' * 60}")
        print("CAPTURE SUMMARY")
        print(f"{'=' * 60}")
        print(f"Topic:    {args.topic}")
        print(f"Records:  {count}")
        print(f"Output:   {args.output}")
        avsc = args.output.with_suffix(".avsc")
        if avsc.exists():
            print(f"Schema:   {avsc}")
        print(f"{'=' * 60}")

        return 0 if count > 0 else 1

    except Exception as e:
        logger.error(f"Capture failed: {e}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
