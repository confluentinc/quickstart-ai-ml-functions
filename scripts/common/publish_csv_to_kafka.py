#!/usr/bin/env python3
"""
Generic CSV-to-Kafka publisher using Avro serialization via Confluent Schema Registry.

Reads a CSV file and publishes each row as an Avro-serialized Kafka message.
Credentials are loaded automatically from the Terraform state in this repo
(terraform/core/terraform.tfstate) — no manual copy-paste required.

Schema resolution order:
  1. --schema-file <path>            — explicit Avro schema JSON file
  2. <csv_file_stem>.avsc             — auto-discovered alongside the CSV
                                        (written by capture_kafka_to_csv.py)
  3. Auto-generated from CSV headers — all fields become nullable strings,
                                        except columns named via --timestamp-columns
                                        which become timestamp-millis longs.

Usage:
    # Minimal — credentials from terraform, schema auto-generated:
    python -m scripts.common.publish_csv_to_kafka \\
        --csv-file data/fema_claims.csv --topic fema_claims --key-column claim_id

    # With explicit schema (e.g. from a previous capture):
    python -m scripts.common.publish_csv_to_kafka \\
        --csv-file data/fema_claims.csv --topic fema_claims \\
        --key-column claim_id --schema-file data/fema_claims.avsc

    # Dry run:
    python -m scripts.common.publish_csv_to_kafka \\
        --csv-file data/fema_claims.csv --topic fema_claims --dry-run

Library usage:
    from scripts.common.publish_csv_to_kafka import CsvKafkaPublisher
    publisher = CsvKafkaPublisher(bootstrap_servers=..., api_key=..., api_secret=...,
                                  schema_registry_url=..., sr_api_key=..., sr_api_secret=...)
    results = publisher.publish_csv_file(
        csv_file=Path("data.csv"), topic="my_topic",
        key_column="id", schema_str=open("schema.avsc").read()
    )
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def infer_avro_schema(
    fieldnames: List[str],
    key_column: Optional[str] = None,
    timestamp_columns: Optional[List[str]] = None,
    record_name: str = "record_value",
    namespace: str = "org.apache.flink.avro.generated.record",
) -> str:
    """
    Auto-generate an Avro schema JSON string from CSV column names.

    Rules:
    - The key column (if given) becomes a required non-null string.
    - Columns in timestamp_columns become timestamp-millis longs.
    - All other columns become nullable strings with default null.

    Args:
        fieldnames:        CSV column headers.
        key_column:        Column used as the Kafka message key.
        timestamp_columns: Columns that hold ISO-8601 timestamp strings
                           to be encoded as Avro timestamp-millis.
        record_name:       Avro record name.
        namespace:         Avro namespace.

    Returns:
        JSON string of the Avro schema.
    """
    ts_cols = set(timestamp_columns or [])
    fields = []
    for name in fieldnames:
        if name in ts_cols:
            fields.append({
                "name": name,
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            })
        elif name == key_column:
            fields.append({"name": name, "type": "string"})
        else:
            fields.append({"name": name, "type": ["null", "string"], "default": None})

    schema = {
        "type": "record",
        "name": record_name,
        "namespace": namespace,
        "fields": fields,
    }
    return json.dumps(schema, indent=2)


def _detect_timestamp_columns(schema_str: str) -> List[str]:
    """Return column names that have logicalType timestamp-millis in the schema."""
    schema = json.loads(schema_str)
    ts_cols = []
    for field in schema.get("fields", []):
        ftype = field.get("type", {})
        # Handle union types: ["null", {"type": "long", "logicalType": "..."}]
        if isinstance(ftype, list):
            for t in ftype:
                if isinstance(t, dict) and t.get("logicalType") == "timestamp-millis":
                    ts_cols.append(field["name"])
                    break
        elif isinstance(ftype, dict) and ftype.get("logicalType") == "timestamp-millis":
            ts_cols.append(field["name"])
    return ts_cols


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------

class CsvKafkaPublisher:
    """
    Reads a CSV file and publishes each row to a Kafka topic using Avro serialization.

    Credentials are passed in directly. Use `from_terraform()` to construct with
    credentials auto-loaded from this repo's Terraform state.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        api_key: str,
        api_secret: str,
        schema_registry_url: str,
        sr_api_key: str,
        sr_api_secret: str,
        dry_run: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.api_key = api_key
        self.api_secret = api_secret
        self.schema_registry_url = schema_registry_url
        self.sr_api_key = sr_api_key
        self.sr_api_secret = sr_api_secret
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

        self._sr_client: Optional[SchemaRegistryClient] = None
        if not dry_run:
            self._sr_client = SchemaRegistryClient({
                "url": schema_registry_url,
                "basic.auth.user.info": f"{sr_api_key}:{sr_api_secret}",
            })

    # ------------------------------------------------------------------
    # Class-method constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_terraform(cls, dry_run: bool = False) -> "CsvKafkaPublisher":
        """
        Construct a publisher with credentials loaded from this repo's Terraform state.

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
            dry_run=dry_run,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_csv_file(
        self,
        csv_file: Path,
        topic: str,
        key_column: Optional[str] = None,
        schema_str: Optional[str] = None,
        timestamp_columns: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Publish all rows from *csv_file* to *topic* using Avro serialization.

        Args:
            csv_file:          Path to the CSV file.
            topic:             Destination Kafka topic name.
            key_column:        CSV column to use as the Kafka message key.
                               Defaults to the first column.
            schema_str:        Avro schema JSON string. If None, auto-generated
                               from CSV headers.
            timestamp_columns: CSV columns containing ISO-8601 timestamps that
                               should be encoded as Avro timestamp-millis longs.
                               Auto-detected from schema_str if not provided.

        Returns:
            Dict with keys "total", "success", "failed".
        """
        results = {"total": 0, "success": 0, "failed": 0}

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                fieldnames = list(reader.fieldnames or [])
                if not fieldnames and rows:
                    fieldnames = list(rows[0].keys())
        except OSError as e:
            self.logger.error(f"Cannot read CSV file {csv_file}: {e}")
            return results

        if not rows:
            self.logger.warning(f"No rows found in {csv_file}")
            return results

        results["total"] = len(rows)

        # Resolve key column
        if key_column is None:
            key_column = fieldnames[0] if fieldnames else None
            self.logger.info(f"No key column specified — using first column: '{key_column}'")

        # Resolve schema
        if schema_str is None:
            schema_str = infer_avro_schema(
                fieldnames=fieldnames,
                key_column=key_column,
                timestamp_columns=timestamp_columns,
            )
            self.logger.info("Auto-generated Avro schema from CSV headers.")

        # Resolve timestamp columns from schema if not explicitly provided
        if timestamp_columns is None:
            timestamp_columns = _detect_timestamp_columns(schema_str)
            if timestamp_columns:
                self.logger.info(f"Timestamp columns (from schema): {timestamp_columns}")

        ts_col_set = set(timestamp_columns or [])

        # Build converter function
        def row_to_avro(record: Dict[str, Any], ctx) -> Dict[str, Any]:
            out = {}
            for k, v in record.items():
                if k in ts_col_set and v:
                    try:
                        dt = datetime.fromisoformat(v)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        else:
                            dt = dt.astimezone(timezone.utc)
                        out[k] = int(dt.timestamp() * 1000)
                    except ValueError:
                        out[k] = v  # leave as-is if unparseable
                else:
                    out[k] = v if v != "" else None
            return out

        self.logger.info(
            f"Publishing {len(rows)} rows from {csv_file} to topic '{topic}' "
            f"(key={key_column})"
        )

        if self.dry_run:
            self.logger.info("[DRY RUN] No messages will be published.")
            results["success"] = len(rows)
            return results

        # Build SerializingProducer
        avro_serializer = AvroSerializer(self._sr_client, schema_str, row_to_avro)
        string_serializer = StringSerializer("utf_8")

        producer = SerializingProducer({
            "bootstrap.servers": self.bootstrap_servers,
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
            "key.serializer": string_serializer,
            "value.serializer": avro_serializer,
        })

        def _delivery_cb(err, msg):
            if err:
                self.logger.error(f"Delivery failed: {err}")

        for idx, row in enumerate(rows, 1):
            try:
                key_val = row.get(key_column, "") if key_column else None
                producer.produce(
                    topic=topic,
                    key=key_val,
                    value=row,
                    on_delivery=_delivery_cb,
                )
                results["success"] += 1
            except Exception as e:
                self.logger.error(f"Row {idx} failed: {e}")
                results["failed"] += 1

            if idx % 100 == 0:
                producer.poll(0)
            if idx % 1000 == 0:
                producer.flush()
                self.logger.info(
                    f"Progress: {idx}/{results['total']} "
                    f"({results['success']} ok, {results['failed']} failed)"
                )

        self.logger.info("Flushing remaining messages...")
        producer.flush()
        return results


# ---------------------------------------------------------------------------
# Schema resolution helper for main()
# ---------------------------------------------------------------------------

def _resolve_schema(args, csv_file: Path, fieldnames: List[str]) -> Optional[str]:
    """Resolve Avro schema: explicit file → sibling .avsc → auto-generate."""
    logger = logging.getLogger(__name__)

    if args.schema_file:
        schema_path = Path(args.schema_file)
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            return None
        schema_str = schema_path.read_text(encoding="utf-8")
        logger.info(f"Loaded schema from {schema_path}")
        return schema_str

    # Auto-discover sibling .avsc file (written by capture_kafka_to_csv.py)
    sibling_avsc = csv_file.with_suffix(".avsc")
    if sibling_avsc.exists():
        schema_str = sibling_avsc.read_text(encoding="utf-8")
        logger.info(f"Auto-discovered schema from {sibling_avsc}")
        return schema_str

    # Fall back: auto-generate from headers
    ts_cols = args.timestamp_columns.split(",") if args.timestamp_columns else []
    schema_str = infer_avro_schema(
        fieldnames=fieldnames,
        key_column=args.key_column,
        timestamp_columns=ts_cols or None,
    )
    logger.info("Auto-generated Avro schema from CSV headers (all fields → nullable string).")
    return schema_str


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="publish_csv_to_kafka",
        description=(
            "Publish a CSV file to a Confluent Cloud Kafka topic via Avro serialization. "
            "Credentials are auto-loaded from terraform/core/terraform.tfstate."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-detect everything (schema from sibling .avsc or auto-generated):
  %(prog)s --csv-file data/fema_claims.csv --topic fema_claims --key-column claim_id

  # Explicit schema file:
  %(prog)s --csv-file data/fema_claims.csv --topic fema_claims \\
      --key-column claim_id --schema-file data/fema_claims.avsc

  # Mark timestamp columns explicitly (when schema is auto-generated):
  %(prog)s --csv-file data/events.csv --topic events \\
      --key-column event_id --timestamp-columns event_time,created_at

  # Dry run — validate without publishing:
  %(prog)s --csv-file data/fema_claims.csv --topic fema_claims --dry-run
        """,
    )
    parser.add_argument("--csv-file", type=Path, required=True,
                        help="CSV file to publish")
    parser.add_argument("--topic", required=True,
                        help="Destination Kafka topic name")
    parser.add_argument("--key-column",
                        help="CSV column to use as the Kafka message key "
                             "(default: first column)")
    parser.add_argument("--schema-file",
                        help="Avro schema JSON file (.avsc). If omitted, auto-discovered "
                             "from a sibling .avsc file or generated from CSV headers.")
    parser.add_argument("--timestamp-columns",
                        help="Comma-separated list of CSV columns holding ISO-8601 "
                             "timestamps to encode as timestamp-millis "
                             "(auto-detected from schema if schema is provided)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate inputs without publishing any messages")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose/debug logging")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error(
            "confluent-kafka is not installed. "
            "Run: uv pip install confluent-kafka"
        )
        return 1

    if not args.csv_file.exists():
        logger.error(f"CSV file not found: {args.csv_file}")
        return 1

    # Peek at headers for schema resolution
    try:
        with open(args.csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            _ = next(reader)  # advance to get fieldnames
            fieldnames = list(reader.fieldnames or [])
    except Exception as e:
        logger.error(f"Cannot read CSV headers: {e}")
        return 1

    schema_str = _resolve_schema(args, args.csv_file, fieldnames)
    if schema_str is None:
        return 1

    ts_cols = None
    if args.timestamp_columns:
        ts_cols = [c.strip() for c in args.timestamp_columns.split(",")]

    # Build publisher from terraform state
    try:
        publisher = CsvKafkaPublisher.from_terraform(dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"Failed to load credentials from Terraform state: {e}")
        logger.error("Ensure terraform/core/terraform.tfstate exists and contains Kafka outputs.")
        return 1

    results = publisher.publish_csv_file(
        csv_file=args.csv_file,
        topic=args.topic,
        key_column=args.key_column,
        schema_str=schema_str,
        timestamp_columns=ts_cols,
    )

    print(f"\n{'=' * 60}")
    print("PUBLISH SUMMARY")
    print(f"{'=' * 60}")
    print(f"Topic:    {args.topic}")
    print(f"File:     {args.csv_file}")
    print(f"Total:    {results['total']}")
    print(f"Success:  {results['success']}")
    print(f"Failed:   {results['failed']}")
    if args.dry_run:
        print("\n[DRY RUN — no messages were sent]")
    print(f"{'=' * 60}")

    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
