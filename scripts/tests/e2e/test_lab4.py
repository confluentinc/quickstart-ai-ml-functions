"""Lab 4 E2E test: validate the walkthrough end-to-end.

  1. `uv run deploy` (via the session fixture) has loaded ~3 days of history.
  2. Every pipeline statement in Lab4-Walkthrough.md runs, and AI_FORECAST
     produces complete 216-step (36-hour) forecasts.
  3. A simulated fault injected into the live stream is flagged by
     AI_DETECT_ANOMALIES with a deviation above the alert threshold.
  4. `uv run lab4-dashboard` serves the forecast and readings.

Requires the AI_FORECAST / AI_DETECT_ANOMALIES Early Access for the org.
"""

import json
import os
import signal
import subprocess
import threading
import time
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from scripts.common.sql_extractors import extract_sql_blocks, extract_sql_object, is_executable_pipeline_sql
from scripts.lab4 import replay
from scripts.tests.helpers.kafka_helper import poll_topic
from scripts.tests.helpers.poll import poll_until

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
WALKTHROUGH = PROJECT_ROOT / "Lab4-Walkthrough.md"
HORIZON = 216
ALERT_THRESHOLD = 2.0
DASHBOARD_PORT = 8765


class TestLab4:
    """Lab 4 E2E: history, both AI pipelines, a simulated fault, and the dashboard."""

    @pytest.mark.order(1)
    def test_history_loaded_by_deploy(self, deployed_environment: dict[str, Any]) -> None:
        """Deploy loads enough history for AI_FORECAST's 4,096-minute context."""
        count = replay.readings_in_topic(deployed_environment["kafka_creds"])
        assert count >= replay.DEFAULT_BACKFILL_ROWS, (
            f"Expected >= {replay.DEFAULT_BACKFILL_ROWS} readings, found {count}"
        )

    @pytest.mark.order(2)
    def test_walkthrough_pipeline(self, deployed_environment: dict[str, Any]) -> None:
        """Run every pipeline block from the walkthrough; forecasts arrive complete."""
        flink = deployed_environment["flink"]
        kafka_creds = deployed_environment["kafka_creds"]

        blocks = [b for b in extract_sql_blocks(WALKTHROUGH) if is_executable_pipeline_sql(b["sql"])]
        assert blocks, "No pipeline SQL found in Lab4-Walkthrough.md"
        submitted = []
        for i, block in enumerate(blocks):
            obj = extract_sql_object(block["sql"])
            name = f"test-lab4-{i}-{(obj[1] if obj else 'block').replace('_', '-')}"[:60]
            print(f"[{i + 1}/{len(blocks)}] {name}: {block['header']}")
            flink.ensure_statement(name, block["sql"], timeout=300)
            submitted.append(name)

        rows = poll_until(
            getter=lambda: poll_topic(kafka_creds, "etth1_forecasts", HORIZON, 60),
            condition=lambda msgs: len(msgs) >= HORIZON,
            timeout=900,
            interval=30,
            description=f"etth1_forecasts has >= {HORIZON} rows",
        )
        per_issue = Counter(str(r["issued_at"]) for r in rows)
        issued, steps = per_issue.most_common(1)[0]
        assert steps == HORIZON, f"Forecast issued {issued} has {steps} steps, expected {HORIZON}"
        first = [r for r in rows if str(r["issued_at"]) == issued]
        assert sorted(r["horizon_step"] for r in first) == list(range(1, HORIZON + 1))
        assert all(r["forecast_q10"] <= r["forecast_q90"] for r in first), "q10 must not exceed q90"
        flink.verify_no_failed(submitted)

    @pytest.mark.order(3)
    def test_simulated_fault_is_flagged(self, deployed_environment: dict[str, Any]) -> None:
        """Inject an overload into the live stream; AI_DETECT_ANOMALIES flags it."""
        kafka_creds = deployed_environment["kafka_creds"]
        publisher, creds = replay.connect()
        readings = replay.load_readings()
        start = replay.readings_in_topic(creds)
        faults = replay.FaultInjector()
        faults.trigger("overload")
        stop = threading.Event()
        # A clean stretch after the fault so the watermark moves past the faulted readings.
        tail = readings[: start + replay.FAULTS["overload"].total_minutes + 30]
        worker = threading.Thread(target=replay.live, args=(publisher, tail, start, 5.0, stop, None, faults))
        worker.start()
        try:
            rows = poll_until(
                # poll_topic stops at min_count, so ask for "everything" and let the
                # 45 s timeout end the read: the fault rows are at the end of the topic.
                getter=lambda: poll_topic(kafka_creds, "etth1_anomalies", 10**9, 45),
                condition=lambda msgs: any(
                    m.get("simulated_fault") and m.get("is_anomaly") and abs(m.get("deviation") or 0) > ALERT_THRESHOLD
                    for m in msgs
                ),
                timeout=1200,
                interval=30,
                description="a simulated fault is flagged in etth1_anomalies",
            )
        finally:
            stop.set()
            worker.join(timeout=60)
        real_alerts = [
            m
            for m in rows
            if m.get("is_anomaly") and not m.get("simulated_fault") and abs(m.get("deviation") or 0) > ALERT_THRESHOLD
        ]
        assert not real_alerts, f"Unexpected alerts on real (unmodified) readings: {real_alerts[:3]}"

    @pytest.mark.order(4)
    def test_dashboard_serves_state(self, deployed_environment: dict[str, Any]) -> None:
        """The dashboard starts, reads the Lab 4 tables, and serves them as JSON."""
        proc = subprocess.Popen(
            ["uv", "run", "lab4-dashboard", "--no-browser", "--no-replay", "--port", str(DASHBOARD_PORT)],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
        try:

            def state() -> dict[str, Any]:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{DASHBOARD_PORT}/api/state", timeout=5) as r:
                        return json.load(r)
                except OSError:
                    return {}

            snap = poll_until(
                getter=state,
                condition=lambda s: bool(s.get("actual")) and len(s.get("forecast", [])) == HORIZON,
                timeout=180,
                interval=5,
                description="dashboard shows readings and a complete forecast",
            )
            assert snap["counts"]["etth1_anomalies"] > 0
            with urllib.request.urlopen(f"http://127.0.0.1:{DASHBOARD_PORT}/", timeout=5) as r:
                assert b"Transformer ETT-1" in r.read()
        finally:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            else:
                proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
            time.sleep(1)
