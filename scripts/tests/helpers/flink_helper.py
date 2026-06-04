"""Flink SQL execution via Confluent CLI for test validation.

Two gotchas that shape this module:

1. `confluent flink statement create --wait` exits 0 for both COMPLETED and FAILED.
   Trusting the exit code lets silent failures through. We poll status via
   `statement describe -o json` after submit, and parse CLI stdout for "| FAILED"
   as defense-in-depth in `verify_sql_object_exists`.

2. A Flink statement and the SQL catalog object it creates are independent. A
   statement can be GC'd after COMPLETED while the TABLE/AGENT/TOOL it created
   persists in the catalog. To assert "the table actually exists," use
   `verify_sql_object_exists(...)` which runs DESCRIBE, not statement status.
"""

import json
import subprocess
import time
import uuid

from scripts.common.sql_extractors import extract_sql_object


class FlinkHelper:
    """Execute and manage Flink SQL statements via the Confluent CLI."""

    def __init__(self, kafka_creds: dict[str, str]) -> None:
        """Initialize from kafka_creds returned by extract_kafka_credentials().

        Args:
            kafka_creds: Credentials dict; must include environment_id, compute_pool_id,
                         cloud, region, cluster_name, service_account_id
        """
        self.environment_id = kafka_creds["environment_id"]
        self.compute_pool_id = kafka_creds["compute_pool_id"]
        self.cloud = kafka_creds["cloud"]
        self.region = kafka_creds["region"]
        self.database = kafka_creds["cluster_name"]
        self.service_account_id = kafka_creds["service_account_id"]
        self.created_statements: list[str] = []
        self.created_sql_objects: list[tuple[str, str]] = []  # (TYPE, name) for cleanup DROPs

    def execute_statement(self, name: str, sql: str) -> str:
        """Submit a Flink SQL statement via the Confluent CLI.

        Tracks both the statement name and any SQL catalog object it creates so
        cleanup_all() can drop them.

        Args:
            name: Unique statement name (alphanumeric + hyphens)
            sql: SQL to execute

        Returns:
            Statement name

        Raises:
            RuntimeError: If the CLI command fails (statement could not be created)
        """
        cmd = [
            "confluent",
            "flink",
            "statement",
            "create",
            name,
            "--sql",
            sql,
            "--compute-pool",
            self.compute_pool_id,
            "--environment",
            self.environment_id,
            "--database",
            self.database,
            "--service-account",
            self.service_account_id,
        ]
        self._delete_statement(name)  # no-op if it doesn't exist
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to create Flink statement '{name}':\nSTDOUT: {e.stdout}\nSTDERR: {e.stderr}"
            ) from e
        self.created_statements.append(name)

        obj = extract_sql_object(sql)
        if obj and obj not in self.created_sql_objects:
            self.created_sql_objects.append(obj)

        return name

    def _get_status(self, name: str) -> str:
        """Return the current status string for a statement.

        Returns 'NOT_FOUND' if the statement does not exist (e.g., was GC'd),
        'UNKNOWN' on unexpected CLI output.
        """
        cmd = [
            "confluent",
            "flink",
            "statement",
            "describe",
            name,
            "--environment",
            self.environment_id,
            "--cloud",
            self.cloud,
            "--region",
            self.region,
            "-o",
            "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return "NOT_FOUND"

        json_start = result.stdout.find("{")
        if json_start == -1:
            return "UNKNOWN"
        try:
            output = json.loads(result.stdout[json_start:])
        except json.JSONDecodeError:
            return "UNKNOWN"
        return str(output.get("status", "UNKNOWN"))

    def wait_for_running_or_completed(self, name: str, timeout: int = 120) -> str:
        """Poll until the statement is RUNNING or COMPLETED, or raise on failure/timeout."""
        return self._wait_for(name, ("RUNNING", "COMPLETED"), timeout)

    def wait_for_running(self, name: str, timeout: int = 120) -> None:
        """Poll until the statement is RUNNING, or raise on failure/timeout."""
        self._wait_for(name, ("RUNNING",), timeout)

    def _wait_for(self, name: str, targets: tuple[str, ...], timeout: int) -> str:
        start = time.time()
        while True:
            status = self._get_status(name)
            if status in targets:
                return status
            if status in ("FAILED", "STOPPED", "DEGRADED", "NOT_FOUND"):
                raise RuntimeError(f"Statement '{name}' reached terminal state: {status}")

            elapsed = time.time() - start
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Timeout waiting for '{name}' to reach {targets} after {elapsed:.1f}s. Current status: {status}"
                )
            time.sleep(5)

    def drop_table_if_exists(self, table: str) -> None:
        """Drop a Flink table if it exists, blocking until the statement completes."""
        name = f"drop-{table.replace('_', '-')}-{int(time.time())}"
        self.execute_statement(name, f"DROP TABLE IF EXISTS `{table}`")
        self.wait_for_running_or_completed(name, timeout=60)

    def verify_sql_object_exists(self, obj_type: str, obj_name: str) -> bool:
        """Verify a SQL catalog object (TABLE, AGENT, TOOL, MODEL) exists via DESCRIBE.

        Runs DESCRIBE [TYPE] name as a synchronous Flink statement and parses the
        CLI output. The CLI exits 0 for both COMPLETED and FAILED — so we parse
        the rendered status column rather than trusting the exit code.

        Args:
            obj_type: "TABLE", "AGENT", "TOOL", or "MODEL"
            obj_name: Unqualified object name

        Returns:
            True if the catalog object exists; False on FAILED, error, or missing.
        """
        stmt_name = self._unique_statement_name(f"verify-{obj_type.lower()}", obj_name)
        # DESCRIBE TABLE foo is invalid syntax; for TABLE just use DESCRIBE foo.
        describe_sql = f"DESCRIBE {obj_name}" if obj_type == "TABLE" else f"DESCRIBE {obj_type} {obj_name}"
        cmd = [
            "confluent",
            "flink",
            "statement",
            "create",
            stmt_name,
            "--sql",
            describe_sql,
            "--compute-pool",
            self.compute_pool_id,
            "--environment",
            self.environment_id,
            "--database",
            self.database,
            "--service-account",
            self.service_account_id,
            "--wait",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            combined = result.stdout + result.stderr
            if "| COMPLETED" in combined:
                return True
            return False
        except Exception:
            return False
        finally:
            self._delete_statement(stmt_name)

    def ensure_statement(self, name: str, sql: str, timeout: int = 300) -> None:
        """Submit a DDL/CTAS statement idempotently and verify the catalog object exists.

        - If the statement already exists in RUNNING/COMPLETED and the catalog
          object is present, this is a no-op.
        - If a stale catalog object exists from a prior run, DROP it first
          (DDL fails on "already exists").
        - After submit, wait for RUNNING or COMPLETED. Then re-verify the
          catalog object exists via DESCRIBE — that's the real assertion.

        Raises:
            AssertionError: catalog object was not created (statement may have FAILED)
        """
        obj = extract_sql_object(sql)

        # Short-circuit if the statement is already healthy AND its object exists.
        status = self._get_status(name)
        if status in ("RUNNING", "COMPLETED"):
            if not obj or self.verify_sql_object_exists(*obj):
                return
            self._delete_statement(name)
        elif status in ("FAILED", "STOPPED", "DEGRADED"):
            self._delete_statement(name)

        # Pre-drop stale catalog object so CREATE doesn't fail with "already exists".
        if obj:
            obj_type, obj_name = obj
            try:
                drop_name = self._unique_statement_name("pre-drop", obj_name)
                self.execute_statement(drop_name, f"DROP {obj_type} IF EXISTS `{obj_name}`")
                self.wait_for_running_or_completed(drop_name, timeout=60)
                self._delete_statement(drop_name)
            except Exception:
                pass  # Best-effort cleanup; if it fails the CREATE below will tell us

        # Submit and wait. Tolerate errors here — the assertion is the DESCRIBE below.
        try:
            self.execute_statement(name, sql)
            self.wait_for_running_or_completed(name, timeout=timeout)
        except (subprocess.CalledProcessError, RuntimeError, TimeoutError):
            pass

        if obj and not self.verify_sql_object_exists(*obj):
            obj_type, obj_name = obj
            status = self._get_status(name)
            raise AssertionError(
                f"{obj_type} `{obj_name}` was not created by statement '{name}' (statement status: {status})"
            )

    def verify_no_failed(self, statement_names: list[str]) -> None:
        """Assert that none of the given statements are in a terminal failure state."""
        bad = []
        for name in statement_names:
            status = self._get_status(name)
            if status in ("FAILED", "STOPPED", "DEGRADED"):
                bad.append(f"{name}={status}")
        if bad:
            raise AssertionError(f"Statements in failure state: {', '.join(bad)}")

    def _delete_statement(self, name: str) -> None:
        """Delete a statement; ignores errors (statement may already be gone)."""
        cmd = [
            "confluent",
            "flink",
            "statement",
            "delete",
            name,
            "--environment",
            self.environment_id,
            "--cloud",
            self.cloud,
            "--region",
            self.region,
            "--force",
        ]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.created_statements = [s for s in self.created_statements if s != name]
        except subprocess.CalledProcessError:
            pass

    def cleanup_all(self) -> None:
        """DROP all SQL objects created by this helper, then delete the statements."""
        for obj_type, obj_name in list(self.created_sql_objects):
            try:
                drop_name = self._unique_statement_name("cleanup-drop", obj_name)
                self.execute_statement(drop_name, f"DROP {obj_type} IF EXISTS `{obj_name}`")
                self.wait_for_running_or_completed(drop_name, timeout=60)
                self._delete_statement(drop_name)
            except Exception:
                pass

        for name in list(self.created_statements):
            self._delete_statement(name)

    @staticmethod
    def _unique_statement_name(prefix: str, obj_name: str) -> str:
        """Return a unique Flink statement name within the CLI's identifier limits."""
        safe_name = obj_name.lower().replace("_", "-")
        return f"{prefix}-{safe_name[:40]}-{uuid.uuid4().hex[:8]}"
