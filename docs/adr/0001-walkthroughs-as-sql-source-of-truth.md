# 0001: Walkthroughs are the single source of truth for lab SQL

**Status:** Accepted (2026-06-04)

## Context

Each Lab in this repo has both:

- A **walkthrough** (`Lab{N}-Walkthrough.md`) — the markdown a user reads and copy-pastes SQL from into the Confluent Cloud Flink UI. This is the user-facing artifact.
- One or more **runtime artifacts** that need the same SQL — the pytest E2E suite (`scripts/tests/e2e/test_lab{1,2,3}.py`), and for Lab 3 specifically, the helper script `scripts/lab3/lab3_flink.py` that submits the three CTAS pipelines to Flink after datagen has started.

Before 2026-06 every artifact had its own hardcoded copy of the SQL. This drifted in practice — Lab 2's walkthrough was migrated to `CREATE OR ALTER MATERIALIZED TABLE payments_flagged …` while its test still ran `CREATE TABLE payments_flagged AS …`. The drift was silent: the test passed (because plain CTAS also works), but the SQL the test exercised was no longer the SQL a user would run.

## Decision

The walkthroughs own the SQL. Tests and runtime scripts parse it from the markdown.

- `scripts/common/sql_extractors.py` provides `extract_sql_blocks(md_path)` and `extract_sql_object(sql)`.
- Tests iterate the extracted blocks in document order, filter to executable DDL/DML via `is_executable_pipeline_sql`, and submit each via `FlinkHelper.ensure_statement` (which is idempotent and verifies the catalog object via DESCRIBE).
- `scripts/lab3/lab3_flink.py` reads the same walkthrough and submits the same blocks via the `confluent` CLI.

Where the walkthrough contains skeleton/template SQL that should not be executed (e.g., the Lab 2 challenge with a `<YOUR_LOGIC>` placeholder), the code fence is ```sql no-parse``` so the extractor skips it.

## Consequences

- Adding a new SQL block to a walkthrough automatically extends test coverage. No second edit required.
- If a SQL block doesn't compile, the test catches it. There's no longer a path where the test SQL works but the walkthrough SQL is broken.
- A guard test (`scripts/tests/e2e/test_sql_extraction.py`) asserts that the regex still parses each walkthrough and that the expected output tables appear in extracted blocks. If a walkthrough edit accidentally breaks the code-fence syntax, this fails in <1 second.
- Lab 4 is empty; the extractor returns no blocks, so tests don't try to run anything.

## Notes

- The same pattern is in use in the sister repo `quickstart-streaming-agents` (where it was originally developed and battle-tested against MCP-server-backed agent pipelines). The ML-functions repo's extractor is a slimmed copy — no AGENT / TOOL / MODEL support is needed here.
