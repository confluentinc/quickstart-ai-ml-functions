#!/usr/bin/env python3
"""
Lab 3 — Tower Traffic Data Generator

Simulates 10 cell towers across 3 regions (NYC, Chicago, LA) with realistic
daily traffic patterns (morning/evening peaks, nighttime dips) and publishes
JSON messages to the `lab3_tower_traffic` Kafka topic at a configurable rate.

Each message represents one telemetry snapshot per tower and includes:
  tower_id, region, ts_ms (epoch ms), throughput_mbps, active_users,
  signal_strength_dbm, capacity_mbps

Credentials are loaded automatically from terraform/core/terraform.tfstate —
no manual copy-paste required.

Usage:
    uv run lab3-datagen                         # backfill 60 min, then live (realtime)
    uv run lab3-datagen --scenario peak         # pin to 18:30 UTC — alerts fire reliably
    uv run lab3-datagen --scenario cycle        # 24h compressed into ~6 min — peaks/troughs live
    uv run lab3-datagen --backfill-minutes 90   # more history for slower workshops
    uv run lab3-datagen --no-backfill           # skip backfill, live mode only
    uv run lab3-datagen --interval 0.5          # faster live cadence
    uv run lab3-datagen --verbose               # per-message delivery logs
"""

import argparse
import json
import math
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker

try:
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from scripts.common.logging_utils import setup_logging
from scripts.common.terraform import extract_kafka_credentials, get_project_root

TOPIC = "lab3_tower_traffic"

# AVRO Schema for tower traffic records (must match Terraform schema registration)
TOWER_TRAFFIC_SCHEMA = """{
  "type": "record",
  "name": "TowerTraffic",
  "namespace": "io.confluent.lab3",
  "fields": [
    {"name": "tower_id", "type": "string"},
    {"name": "region", "type": "string"},
    {"name": "ts_ms", "type": "long"},
    {"name": "throughput_mbps", "type": "double"},
    {"name": "active_users", "type": "int"},
    {"name": "signal_strength_dbm", "type": "int"},
    {"name": "capacity_mbps", "type": "double"}
  ]
}"""

# 10 towers across 3 regions — capacity_mbps is the per-tower ceiling used
# by the alert pipeline to compute utilisation percentage.
TOWERS = [
    {"tower_id": "TOWER-NYC-01", "region": "NYC", "capacity_mbps": 1000.0},
    {"tower_id": "TOWER-NYC-02", "region": "NYC", "capacity_mbps": 800.0},
    {"tower_id": "TOWER-NYC-03", "region": "NYC", "capacity_mbps": 1200.0},
    {"tower_id": "TOWER-CHI-01", "region": "CHI", "capacity_mbps": 900.0},
    {"tower_id": "TOWER-CHI-02", "region": "CHI", "capacity_mbps": 750.0},
    {"tower_id": "TOWER-CHI-03", "region": "CHI", "capacity_mbps": 850.0},
    {"tower_id": "TOWER-CHI-04", "region": "CHI", "capacity_mbps": 600.0},
    {"tower_id": "TOWER-LA-01", "region": "LA", "capacity_mbps": 1100.0},
    {"tower_id": "TOWER-LA-02", "region": "LA", "capacity_mbps": 950.0},
    {"tower_id": "TOWER-LA-03", "region": "LA", "capacity_mbps": 700.0},
]


SCENARIOS = ("realtime", "peak", "cycle")
CYCLE_SPEED_FACTOR = 240  # 24 simulated hours per 6 wall-clock minutes

# Pin `peak` mode 90 minutes before the 18:30 evening peak. At this point the
# natural curve sits at ~87% utilization, jitter range [75%, 99%] — well above
# the 85% alert threshold, but NOT clipped at 100%. Clipping at 100% truncates
# ARIMA's training distribution and leads to absurd forecast extrapolations
# (e.g. 131% utilization).
PEAK_HOUR_UTC = 17
PEAK_MINUTE_UTC = 0


def _peak_time(reference: datetime) -> datetime:
    return reference.replace(hour=PEAK_HOUR_UTC, minute=PEAK_MINUTE_UTC, second=0, microsecond=0)


def _scenario_time_sources(scenario: str, backfill_minutes: int):
    """
    Build the time-source callables for a given --scenario.

    Returns (live_now, backfill_at) where:
      live_now()         -> simulated 'now' used in live mode (called once per reading)
      backfill_at(step)  -> simulated time for one backfill step (step is 0-indexed)

    Scenarios:
      realtime  use wall-clock UTC. Peaks fire only ~07-09 and ~17-20 UTC.
      peak      pin every reading to 17:00 UTC (~87% mean utilization). Above
                the 85% alert threshold but below the 100% clipping ceiling,
                so ARIMA forecasts stay sensible.
      cycle     compress 24h into ~6 wall minutes so live mode cycles through
                peaks and troughs and ARIMA learns the full daily curve.
    """
    start = datetime.now(timezone.utc)

    if scenario == "realtime":
        def live_now() -> datetime:
            return datetime.now(timezone.utc)

        def backfill_at(step: int) -> datetime:
            return start - timedelta(minutes=backfill_minutes - step)

        return live_now, backfill_at

    if scenario == "peak":
        peak = _peak_time(start)

        def live_now() -> datetime:
            return _peak_time(datetime.now(timezone.utc))

        def backfill_at(step: int) -> datetime:
            return peak - timedelta(minutes=backfill_minutes - step)

        return live_now, backfill_at

    if scenario == "cycle":
        midnight = start.replace(hour=0, minute=0, second=0, microsecond=0)

        def live_now() -> datetime:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            sim_hours = (elapsed * CYCLE_SPEED_FACTOR / 3600.0) % 24.0
            return midnight + timedelta(hours=sim_hours)

        def backfill_at(step: int) -> datetime:
            # Spread the backfill across one full 24h simulated cycle so ARIMA
            # trains on the entire daily curve before live mode starts.
            denom = max(backfill_minutes - 1, 1)
            sim_hours = (step / denom) * 24.0
            return midnight + timedelta(hours=sim_hours)

        return live_now, backfill_at

    raise ValueError(f"Unknown scenario {scenario!r}; expected one of {SCENARIOS}")


def _traffic_utilization(hour: float) -> float:
    """
    Return a utilisation fraction in [0, 1] for the given fractional hour (0–24).

    Traffic shape:
      - Morning rush  ~07:00–09:00  peak ~85 %
      - Evening rush  ~17:00–20:00  peak ~98 %
      - Late night    ~02:00–05:00  trough ~18 %
    """
    morning = 0.85 * math.exp(-0.5 * ((hour - 8.0) / 1.2) ** 2)
    evening = 0.98 * math.exp(-0.5 * ((hour - 18.5) / 1.8) ** 2)
    base = 0.18
    return min(1.0, base + morning + evening)


def _generate_reading(tower: dict[str, Any], fake: Faker, at: datetime | None = None) -> dict[str, Any]:
    """Return a single telemetry snapshot for one tower at the given time (or now)."""
    now = at or datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0

    # Per-tower utilisation with random jitter (±12 %)
    jitter = fake.pyfloat(min_value=-0.12, max_value=0.12, right_digits=3)
    utilization = min(1.0, max(0.05, _traffic_utilization(hour) + jitter))

    throughput = round(tower["capacity_mbps"] * utilization, 2)

    # Active users scale with utilisation (up to ~5 000 per tower)
    user_jitter = fake.pyfloat(min_value=0.9, max_value=1.1, right_digits=3)
    active_users = int(utilization * 5000 * user_jitter)

    # Signal strength degrades at high load: -65 dBm (light) → -100 dBm (heavy)
    signal_dbm = int(-65 - (utilization * 35)) + fake.pyint(min_value=-3, max_value=3)

    return {
        "tower_id": tower["tower_id"],
        "region": tower["region"],
        # Epoch milliseconds — mapped to TIMESTAMP_LTZ(3) in the Flink table
        "ts_ms": int(now.timestamp() * 1000),
        "throughput_mbps": throughput,
        "active_users": active_users,
        "signal_strength_dbm": signal_dbm,
        "capacity_mbps": tower["capacity_mbps"],
    }


def _backfill(producer, key_serializer, value_serializer, fake: Faker, logger, delivery_cb, backfill_minutes: int, backfill_at) -> int:
    """
    Burst-produce backdated records oldest-first to warm the ARIMA model.

    Produces one snapshot per tower per backfill step (one step ≈ one simulated
    minute in `realtime`/`peak`, or one slice of the 24h curve in `cycle`).
    60 steps × 10 towers = 600 records, sent in ~1–2 seconds.

    Records must be produced in chronological order so Flink's event-time
    watermark advances forward and the TUMBLE windows close in sequence.
    """
    total = 0

    print(
        f"[backfill] Producing {backfill_minutes} steps × {len(TOWERS)} towers = "
        f"{backfill_minutes * len(TOWERS)} records (oldest-first)..."
    )

    for step in range(backfill_minutes):
        at = backfill_at(step)
        for tower in TOWERS:
            reading = _generate_reading(tower, fake, at=at)
            producer.produce(
                topic=TOPIC,
                key=key_serializer(reading["tower_id"], SerializationContext(TOPIC, MessageField.KEY)),
                value=value_serializer(reading, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_cb,
            )
            total += 1
        producer.poll(0)

    producer.flush()
    logger.info("[backfill] Done — %d records published", total)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Lab 3 tower traffic generator — publishes one snapshot per tower "
            "per interval to `lab3_tower_traffic`."
        )
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Pause between full-tower batches (default: 1.0 s)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--backfill-minutes",
        type=int,
        default=60,
        metavar="N",
        help="Minutes of backdated history to burst-produce on startup (default: 60 → 12 windows). "
             "Must be ≥ 10 × 5 = 50 to satisfy ML_FORECAST minTrainingSize=10.",
    )
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="Skip historical backfill and go straight to live mode",
    )
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="realtime",
        help=(
            "Traffic-shape time source: "
            "'realtime' uses wall-clock UTC (default; alerts only fire ~07-09 and ~17-20 UTC). "
            "'peak' pins every reading to 17:00 UTC (~87%% mean utilization, no clipping) so alerts fire reliably. "
            "'cycle' compresses 24h into ~6 wall minutes so live mode cycles through peaks and troughs."
        ),
    )
    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error("confluent-kafka is not installed. Run: uv sync")
        sys.exit(1)

    logger.info("Loading credentials from Terraform state...")
    project_root = get_project_root()
    creds = extract_kafka_credentials("terraform", project_root)

    # Schema Registry client
    schema_registry_conf = {
        "url": creds["schema_registry_url"],
        "basic.auth.user.info": f"{creds['schema_registry_api_key']}:{creds['schema_registry_api_secret']}",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Serializers
    key_serializer = StringSerializer("utf_8")
    value_serializer = AvroSerializer(schema_registry_client, TOWER_TRAFFIC_SCHEMA)

    producer = Producer(
        {
            "bootstrap.servers": creds["bootstrap_servers"],
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": creds["kafka_api_key"],
            "sasl.password": creds["kafka_api_secret"],
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
        }
    )

    def _delivery_cb(err, msg):
        if err:
            logger.error("Delivery failed [%s]: %s", msg.topic(), err)
        elif args.verbose:
            logger.debug(
                "Delivered → %s[%d] offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    fake = Faker()
    running = True

    def _shutdown(sig, frame):
        nonlocal running
        print("\nShutting down...")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    live_now, backfill_at = _scenario_time_sources(args.scenario, args.backfill_minutes)
    print(f"Scenario: {args.scenario}")

    if not args.no_backfill:
        _backfill(producer, key_serializer, value_serializer, fake, logger, _delivery_cb, args.backfill_minutes, backfill_at)
        print("[backfill] Switching to live mode...")

    print(f"Publishing to '{TOPIC}' — {len(TOWERS)} towers — Ctrl+C to stop")
    total = 0

    while running:
        sim_now = live_now()
        for tower in TOWERS:
            if not running:
                break
            reading = _generate_reading(tower, fake, at=sim_now)
            producer.produce(
                topic=TOPIC,
                key=key_serializer(reading["tower_id"], SerializationContext(TOPIC, MessageField.KEY)),
                value=value_serializer(reading, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=_delivery_cb,
            )
            total += 1

        producer.poll(0)

        if total % (len(TOWERS) * 10) == 0:
            logger.info("Published %d messages across %d towers", total, len(TOWERS))

        time.sleep(args.interval)

    producer.flush()
    logger.info("Done. Total messages published: %d", total)


if __name__ == "__main__":
    main()
