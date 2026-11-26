#!/usr/bin/env python3
"""
Lab 4 - Transformer sensor replay.

Streams the ETTh1 transformer readings (scripts/lab4/data/etth1_minutely.csv,
one row per minute, 2017-10-21 to 2017-10-28) into the `etth1_readings` table.

Each record's Kafka timestamp is set to the reading's own time, so Flink sees
the data exactly as the sensor produced it, just faster.

Two phases:
  backfill  Load the first ~3 days in one burst. Both AI pipelines use 3 days
            (4,320 minutes) of context, so this is what makes results appear
            as soon as you run the SQL.
            `uv run deploy lab4` does this for you.
  live      Replay the remaining readings at a steady pace (default 1 per
            second, so one simulated hour per minute). `uv run lab4-dashboard`
            does this for you while the dashboard is open.

Credentials come from terraform/core/terraform.tfstate.

Usage:
    uv run lab4-replay                  # backfill (if needed), then live
    uv run lab4-replay --backfill-only  # just the history
    uv run lab4-replay --speed 5        # live at 5 readings per second
"""

from __future__ import annotations

import argparse
import csv
import logging
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from scripts.common.terraform import extract_kafka_credentials, get_project_root

logger = logging.getLogger(__name__)

TOPIC = "etth1_readings"
TRANSFORMER_ID = "ETT-1"
DATA_FILE = Path(__file__).parent / "data" / "etth1_minutely.csv"
SIGNALS = ("HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT")

# 4,500 minutes = 3.1 days: just over the 3-day (4,320-minute) context both AI
# pipelines use, so results start during the backfill but there is little
# history left for Flink to work through before it is live.
DEFAULT_BACKFILL_ROWS = 4500


@dataclass(frozen=True)
class Reading:
    """One minute of transformer readings."""

    reading_time: datetime
    values: dict[str, float]
    simulated_fault: bool = False

    @property
    def ts_ms(self) -> int:
        return int(self.reading_time.timestamp() * 1000)

    def to_record(self) -> dict[str, Any]:
        return {
            "transformer_id": TRANSFORMER_ID,
            "reading_time": self.reading_time,
            **self.values,
            "simulated_fault": self.simulated_fault,
        }


@dataclass(frozen=True)
class Fault:
    """A simulated fault: HUFL jumps by `offset`, holds for `minutes`, then eases back.

    The abrupt start is what the anomaly check should catch. The gradual recovery
    (`recovery_minutes`) mirrors load being restored step by step, and keeps the
    return to normal from looking like a second, unlabelled fault.
    """

    key: str
    label: str
    offset: float
    minutes: int
    recovery_minutes: int = 10

    def offset_at(self, minute: int) -> float:
        if minute < self.minutes:
            return self.offset
        step = minute - self.minutes + 1
        return self.offset * (1 - step / (self.recovery_minutes + 1))

    @property
    def total_minutes(self) -> int:
        return self.minutes + self.recovery_minutes


# HUFL normally swings between about -12 and +14 over a day and moves at most
# ~0.25 per minute, so an abrupt 8-unit step is unmistakable.
FAULTS = {
    "overload": Fault("overload", "Overload spike", offset=8.0, minutes=20),
    "load_loss": Fault("load_loss", "Sudden load loss", offset=-8.0, minutes=15),
}


class FaultInjector:
    """Thread-safe: the dashboard triggers faults while the replay loop applies them."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: Fault | None = None
        self._minute = 0

    def trigger(self, key: str) -> Fault:
        fault = FAULTS[key]
        with self._lock:
            self._active, self._minute = fault, 0
        return fault

    @property
    def active(self) -> Fault | None:
        with self._lock:
            return self._active

    @property
    def progress(self) -> tuple[Fault, int] | None:
        """The active fault and how many of its readings have been sent."""
        with self._lock:
            return (self._active, self._minute) if self._active else None

    def apply(self, reading: Reading) -> Reading:
        with self._lock:
            if self._active is None:
                return reading
            offset = self._active.offset_at(self._minute)
            self._minute += 1
            if self._minute >= self._active.total_minutes:
                self._active = None
        values = {**reading.values, "HUFL": reading.values["HUFL"] + offset}
        return Reading(reading.reading_time, values, simulated_fault=True)


def load_readings(path: Path = DATA_FILE) -> list[Reading]:
    """Load the dataset. Timestamps are treated as UTC."""
    readings = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            ts = datetime.fromisoformat(row["date"]).replace(tzinfo=timezone.utc)
            readings.append(Reading(ts, {name: float(row[name]) for name in SIGNALS}))
    return readings


class ReadingPublisher:
    """Produces readings to `etth1_readings` using the schema Flink registered for the table."""

    def __init__(self, creds: dict[str, str]):
        self._creds = creds
        sr = SchemaRegistryClient(
            {
                "url": creds["schema_registry_url"],
                "basic.auth.user.info": f"{creds['schema_registry_api_key']}:{creds['schema_registry_api_secret']}",
            }
        )
        subject = f"{TOPIC}-value"
        try:
            schema_str = sr.get_latest_version(subject).schema.schema_str
        except Exception as e:
            raise RuntimeError(
                f"No schema found for '{subject}'. The etth1_readings table is created by "
                "`uv run deploy lab4`; run that first."
            ) from e

        self._serializer = AvroSerializer(
            sr, schema_str, conf={"auto.register.schemas": False, "use.latest.version": True}
        )
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer(
            {
                **_kafka_conf(creds),
                "linger.ms": 20,
                "compression.type": "snappy",
                "enable.idempotence": True,
            }
        )
        self._errors = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err:
            self._errors += 1
            logger.error("Delivery failed: %s", err)

    def publish(self, reading: Reading) -> None:
        self._producer.produce(
            topic=TOPIC,
            key=self._key_serializer(TRANSFORMER_ID, SerializationContext(TOPIC, MessageField.KEY)),
            value=self._serializer(reading.to_record(), SerializationContext(TOPIC, MessageField.VALUE)),
            timestamp=reading.ts_ms,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)
        if self._errors:
            raise RuntimeError(f"{self._errors} readings failed to deliver")


def _kafka_conf(creds: dict[str, str]) -> dict[str, Any]:
    return {
        "bootstrap.servers": creds["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": creds["kafka_api_key"],
        "sasl.password": creds["kafka_api_secret"],
        "log_level": 3,  # librdkafka: errors only; keeps the attendee's terminal readable
    }


def readings_in_topic(creds: dict[str, str]) -> int:
    """How many readings are already in the topic (lets every command be re-run safely)."""
    consumer = Consumer({**_kafka_conf(creds), "group.id": "lab4-offset-check", "enable.auto.commit": False})
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=15)
        return max(0, high - low)
    finally:
        consumer.close()


def backfill(publisher: ReadingPublisher, readings: list[Reading], already: int, target: int) -> int:
    """Publish readings[already:target] as fast as possible. Returns the new count."""
    if already >= target:
        print(f"✓ History already loaded ({already:,} readings in {TOPIC})")
        return already
    batch = readings[already:target]
    print(f"Loading {len(batch):,} readings of history into {TOPIC} ...")
    for reading in batch:
        publisher.publish(reading)
    publisher.flush()
    print(f"✓ History loaded: {batch[0].reading_time:%Y-%m-%d %H:%M} → {batch[-1].reading_time:%Y-%m-%d %H:%M} UTC")
    return target


def live(
    publisher: ReadingPublisher,
    readings: list[Reading],
    start: int,
    speed: float,
    stop: threading.Event,
    on_publish: Callable[[Reading], None] | None = None,
    faults: FaultInjector | None = None,
) -> int:
    """Replay readings[start:] at `speed` readings per second until done or stopped."""
    interval = 1.0 / speed
    sent = start
    for original in _paced(readings[start:], interval, stop):
        reading = faults.apply(original) if faults else original
        publisher.publish(reading)
        sent += 1
        if on_publish:
            on_publish(reading)
    # Stopped mid-fault? Publish the rest of it (including the recovery) now, so the
    # next run doesn't snap back to normal and raise an unlabelled "real" alert.
    while faults is not None and faults.active is not None and sent < len(readings):
        reading = faults.apply(readings[sent])
        publisher.publish(reading)
        sent += 1
        if on_publish:
            on_publish(reading)
    publisher.flush()
    return sent


def _paced(items: list[Reading], interval: float, stop: threading.Event) -> Iterator[Reading]:
    next_at = time.monotonic()
    for item in items:
        if stop.is_set():
            return
        yield item
        next_at += interval
        stop.wait(max(0.0, next_at - time.monotonic()))


def quiet_logs() -> None:
    """The shared Terraform helper logs every step at INFO/WARNING; attendees don't need it."""
    logging.getLogger("scripts.common.terraform").setLevel(logging.ERROR)


def connect() -> tuple[ReadingPublisher, dict[str, str]]:
    quiet_logs()
    creds = extract_kafka_credentials("terraform", get_project_root())
    return ReadingPublisher(creds), creds


def backfill_for_deploy() -> None:
    """Called by `uv run deploy` after the lab4 Terraform apply."""
    publisher, creds = connect()
    backfill(publisher, load_readings(), readings_in_topic(creds), DEFAULT_BACKFILL_ROWS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay ETTh1 transformer readings into Kafka")
    parser.add_argument("--speed", type=float, default=1.0, help="Live readings per second (default: 1)")
    parser.add_argument("--backfill-only", action="store_true", help="Load the history and exit")
    parser.add_argument(
        "--backfill-rows", type=int, default=DEFAULT_BACKFILL_ROWS, help="Rows of history to load first"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")

    readings = load_readings()
    publisher, creds = connect()
    count = backfill(publisher, readings, readings_in_topic(creds), args.backfill_rows)
    if args.backfill_only:
        return
    if count >= len(readings):
        print("✓ All readings already replayed. To start over: uv run destroy && uv run deploy lab4")
        return

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    remaining = len(readings) - count
    print(
        f"Streaming {remaining:,} live readings at {args.speed:g}/s (~{remaining / args.speed / 60:.0f} min). Ctrl+C to stop."
    )

    def progress(r: Reading) -> None:
        print(f"\r  {r.reading_time:%Y-%m-%d %H:%M}  HUFL {r.values['HUFL']:7.2f}", end="", flush=True)

    sent = live(publisher, readings, count, args.speed, stop, progress)
    print(f"\n✓ Stopped at {sent:,}/{len(readings):,} readings. Run again to continue where you left off.")


def cli() -> None:
    """Entry point for `uv run lab4-replay`: plain-English errors instead of tracebacks."""
    try:
        main()
    except (RuntimeError, FileNotFoundError) as e:
        print(f"✗ {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cli()
