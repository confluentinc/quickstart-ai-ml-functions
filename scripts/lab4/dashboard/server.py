#!/usr/bin/env python3
"""
Lab 4 live dashboard.

    uv run lab4-dashboard            # http://localhost:8050

What it does:
  1. Streams the remaining transformer readings into `etth1_readings` (one
     simulated minute per second), applying any fault you trigger in the UI.
  2. Follows the three Lab 4 tables in Kafka (readings, forecasts, anomalies).
  3. Serves one page that charts them and updates every second or so.

Standard library HTTP server + confluent-kafka; no extra dependencies.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import statistics
import sys
import threading
import time
import webbrowser
from collections import deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.lab4 import replay

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
READINGS, FORECASTS, ANOMALIES = "etth1_readings", "etth1_forecasts", "etth1_anomalies"
MINUTE_MS = 60_000
HISTORY_MINUTES = 36 * 60  # how much past the chart shows
FORECAST_HORIZON = 216  # 36 hours of 10-minute steps
# Deviation (in HUFL units) an anomaly needs before it counts as an alert. Matches
# the walkthrough's `ABS(deviation) > 2`: it keeps simulated faults and drops the
# sub-unit blips left by upsampling hourly data to minutes.
ALERT_THRESHOLD = 2.0


def _ms(value: Any) -> int:
    """Avro timestamps arrive as datetimes (aware or naive); treat naive as UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)
    return int(value)


class LabState:
    """Everything the page shows. Written by Kafka/replay threads, read by HTTP threads."""

    def __init__(self, total_readings: int, speed: float):
        self.lock = threading.Lock()
        self.readings: dict[int, tuple[float, bool]] = {}
        self.anomalies: dict[int, tuple[float | None, float | None, float | None, bool, float | None]] = {}
        self.forecasts: dict[int, list[tuple[int, float, float, float]]] = {}
        self.latest_forecast: int | None = None
        self.alerts: deque[dict[str, Any]] = deque(maxlen=50)
        self.latencies: deque[float] = deque(maxlen=20)
        self.published_at: dict[int, float] = {}
        self.counts = {READINGS: 0, FORECASTS: 0, ANOMALIES: 0}
        self.total_readings = total_readings
        self.speed = speed
        self.replay_done = False
        self.replay_error: str | None = None
        self.read_only = False

    # ── writers ──────────────────────────────────────────────────────────────
    def on_reading(self, rec: dict[str, Any]) -> None:
        with self.lock:
            self.readings[_ms(rec["reading_time"])] = (rec["HUFL"], bool(rec.get("simulated_fault")))
            self.counts[READINGS] += 1

    def on_anomaly(self, rec: dict[str, Any]) -> None:
        ts = _ms(rec["reading_time"])
        now = time.time()
        with self.lock:
            self.counts[ANOMALIES] += 1
            flagged = bool(rec.get("is_anomaly"))
            self.anomalies[ts] = (
                rec.get("expected"),
                rec.get("lower_bound"),
                rec.get("upper_bound"),
                flagged,
                rec.get("deviation"),
            )
            published = self.published_at.pop(ts, None)
            latency = now - published if published else None
            deviation = rec.get("deviation") or 0.0
            if flagged and abs(deviation) > ALERT_THRESHOLD:
                if latency is not None:
                    self.latencies.append(latency)
                self.alerts.appendleft(
                    {
                        "ts": ts,
                        "actual": rec.get("actual"),
                        "expected": rec.get("expected"),
                        "deviation": deviation,
                        "simulated": bool(rec.get("simulated_fault")),
                        "latency": latency,
                    }
                )

    def on_forecast(self, rec: dict[str, Any]) -> None:
        issued = _ms(rec["issued_at"])
        with self.lock:
            self.counts[FORECASTS] += 1
            steps = self.forecasts.setdefault(issued, [])
            steps.append((_ms(rec["forecast_time"]), rec["forecast_mean"], rec["forecast_q10"], rec["forecast_q90"]))
            if len(steps) >= FORECAST_HORIZON and (self.latest_forecast is None or issued > self.latest_forecast):
                self.latest_forecast = issued
                for old in [k for k in self.forecasts if k < issued]:
                    del self.forecasts[old]

    def on_published(self, reading: replay.Reading) -> None:
        with self.lock:
            self.published_at[reading.ts_ms] = time.time()
            if len(self.published_at) > 5000:
                for k in sorted(self.published_at)[:1000]:
                    del self.published_at[k]

    # ── reader ───────────────────────────────────────────────────────────────
    def _latest_check(self, ts: int | None) -> dict[str, Any] | None:
        """The most recent reading the anomaly check has scored (it trails the stream by ~1 reading)."""
        if ts is None or ts not in self.readings:
            return None
        expected = self.anomalies[ts][0]
        return {"ts": ts, "actual": self.readings[ts][0], "expected": expected}

    def snapshot(self, faults: replay.FaultInjector) -> dict[str, Any]:
        with self.lock:
            if not self.readings:
                now_ms = None
                actual: list[list[Any]] = []
            else:
                now_ms = max(self.readings)
                start = now_ms - HISTORY_MINUTES * MINUTE_MS
                actual = [[t, round(v, 3), int(sim)] for t, (v, sim) in sorted(self.readings.items()) if t >= start]
            expected = []
            if now_ms is not None:
                start = now_ms - HISTORY_MINUTES * MINUTE_MS
                for t in sorted(k for k in self.anomalies if k >= start):
                    exp, lo, hi, flagged, dev = self.anomalies[t]
                    if exp is not None:
                        expected.append(
                            [
                                t,
                                round(exp, 3),
                                round(lo or exp, 3),
                                round(hi or exp, 3),
                                int(flagged and abs(dev or 0) > ALERT_THRESHOLD),
                            ]
                        )
            forecast = []
            if self.latest_forecast is not None:
                forecast = [
                    [t, round(m, 3), round(lo, 3), round(hi, 3)]
                    for t, m, lo, hi in sorted(self.forecasts[self.latest_forecast])
                ]
            latest_scored = max(self.anomalies) if self.anomalies else None
            current = self.readings.get(now_ms) if now_ms else None
            progress = faults.progress
            last_sim = next((a for a in self.alerts if a["simulated"] and a["latency"] is not None), None)
            return {
                "now": now_ms,
                "actual": actual,
                "expected": expected,
                "forecast": forecast,
                "forecast_issued": self.latest_forecast,
                "alerts": list(self.alerts)[:20],
                "alert_count": len(self.alerts),
                "latency": statistics.median(self.latencies) if self.latencies else None,
                "current": current[0] if current else None,
                "checked": self._latest_check(latest_scored),
                "scored_until": latest_scored,
                "counts": dict(self.counts),
                "replay": {
                    "read_only": self.read_only,
                    "position": len(self.readings),
                    "total": self.total_readings,
                    "speed": self.speed,
                    "done": self.replay_done,
                    "error": self.replay_error,
                },
                "fault": (
                    {
                        "key": progress[0].key,
                        "label": progress[0].label,
                        "done": progress[1],
                        "total": progress[0].total_minutes,
                    }
                    if progress
                    else None
                ),
                "last_detection": (
                    {"ts": last_sim["ts"], "latency": last_sim["latency"], "deviation": last_sim["deviation"]}
                    if last_sim
                    else None
                ),
                "faults": [
                    {"key": f.key, "label": f.label, "offset": f.offset, "minutes": f.minutes}
                    for f in replay.FAULTS.values()
                ],
                "threshold": ALERT_THRESHOLD,
            }


class TableFollower(threading.Thread):
    """Reads one Lab 4 table's topic from the beginning and keeps following it."""

    def __init__(
        self, topic: str, creds: dict[str, str], sr: SchemaRegistryClient, handler: Any, stop: threading.Event
    ):
        super().__init__(daemon=True, name=f"follow-{topic}")
        self.topic, self.handler, self.stop = topic, handler, stop
        self.deserializer = AvroDeserializer(sr)
        self.consumer = Consumer(
            {
                **replay._kafka_conf(creds),
                "group.id": f"lab4-dashboard-{topic}-{int(time.time())}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                # Flink commits its output transactionally about once a minute. Reading
                # uncommitted records shows results the moment Flink writes them.
                "isolation.level": "read_uncommitted",
                "topic.metadata.refresh.interval.ms": 5000,
            }
        )

    def run(self) -> None:
        self.consumer.subscribe([self.topic])
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        while not self.stop.is_set():
            msg = self.consumer.poll(0.5)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.debug("%s: %s", self.topic, err)
                continue
            try:
                record = self.deserializer(msg.value(), ctx)
            except Exception as e:  # schema not registered yet, or a tombstone
                logger.debug("%s: skipped record: %s", self.topic, e)
                continue
            if record:
                self.handler(record)
        self.consumer.close()


class ReplayThread(threading.Thread):
    """Streams the not-yet-published readings, applying faults from the UI."""

    def __init__(self, state: LabState, faults: replay.FaultInjector, speed: float, stop: threading.Event):
        super().__init__(daemon=True, name="replay")
        self.state, self.faults, self.speed, self.stop = state, faults, speed, stop

    def run(self) -> None:
        try:
            publisher, creds = replay.connect()
            readings = replay.load_readings()
            already = replay.readings_in_topic(creds)
            start = replay.backfill(publisher, readings, already, replay.DEFAULT_BACKFILL_ROWS)
            replay.live(publisher, readings, start, self.speed, self.stop, self.state.on_published, self.faults)
        except Exception as e:
            logger.exception("Replay stopped")
            self.state.replay_error = str(e)
        finally:
            self.state.replay_done = True


def make_handler(state: LabState, faults: replay.FaultInjector) -> type[BaseHTTPRequestHandler]:
    index = (STATIC_DIR / "index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            logger.debug(fmt, *args)

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
            self._send(status, json.dumps(payload, separators=(",", ":")).encode(), "application/json")

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path in ("/", "/index.html"):
                self._send(HTTPStatus.OK, index, "text/html; charset=utf-8")
            elif path == "/api/state":
                self._json(state.snapshot(faults))
            elif path == "/healthz":
                self._json({"ok": True})
            else:
                self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            if not path.startswith("/api/fault/"):
                self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")
                return
            key = path.rsplit("/", 1)[-1]
            if key not in replay.FAULTS:
                self._json({"error": f"unknown fault '{key}'"}, HTTPStatus.BAD_REQUEST)
                return
            if state.replay_done:
                self._json(
                    {"error": "The replay has finished, so there is no live stream to inject into."},
                    HTTPStatus.CONFLICT,
                )
                return
            fault = faults.trigger(key)
            print(f"  ⚡ Simulated fault: {fault.label} ({fault.offset:+g} for {fault.minutes} min, then easing back)")
            self._json({"ok": True, "fault": fault.label})

    return Handler


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
    parser = argparse.ArgumentParser(description="Lab 4 live dashboard")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--speed", type=float, default=1.0, help="Live readings per second (default: 1)")
    parser.add_argument("--no-replay", action="store_true", help="Only show data; don't stream new readings")
    parser.add_argument("--no-browser", action="store_true", help="Don't open a browser tab")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")

    replay.quiet_logs()
    try:
        creds = replay.extract_kafka_credentials("terraform", replay.get_project_root())
    except FileNotFoundError:
        print("✗ Nothing deployed yet. Run: uv run deploy lab4")
        raise SystemExit(1) from None

    sr = SchemaRegistryClient(
        {
            "url": creds["schema_registry_url"],
            "basic.auth.user.info": f"{creds['schema_registry_api_key']}:{creds['schema_registry_api_secret']}",
        }
    )
    stop = threading.Event()
    state = LabState(len(replay.load_readings()), args.speed)
    faults = replay.FaultInjector()

    for topic, handler in ((READINGS, state.on_reading), (FORECASTS, state.on_forecast), (ANOMALIES, state.on_anomaly)):
        TableFollower(topic, creds, sr, handler, stop).start()
    if not args.no_replay:
        ReplayThread(state, faults, args.speed, stop).start()
    else:
        state.replay_done = state.read_only = True

    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(state, faults))
    url = f"http://localhost:{args.port}"
    print(f"\n  Lab 4 dashboard → {url}")
    if not args.no_replay:
        print(f"  Streaming live readings at {args.speed:g}/s. Press Ctrl+C to stop.\n")

    def shutdown(*_: Any) -> None:
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    server.serve_forever()
    print("\n  Stopped. Run `uv run lab4-dashboard` again to continue where you left off.")


if __name__ == "__main__":
    main()
