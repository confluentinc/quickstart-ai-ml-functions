"""Extract Flink SQL statements from Lab walkthrough markdown files.

The Lab walkthroughs are the single source of truth for user-facing SQL.
Tests and runtime scripts both extract their SQL from these files to keep
them in lockstep.

Use ```sql no-parse``` (instead of just ```sql```) in walkthrough markdown
for code blocks that should NOT be executed by tests — for example, skeleton
SQL with placeholders like `<YOUR_LOGIC>`.
"""

import re
from pathlib import Path


def extract_sql_blocks(md_path: Path) -> list[dict]:
    """Return every ```sql``` code block from a walkthrough in document order.

    Blocks fenced as ```sql no-parse``` are skipped (use them for skeleton SQL
    or examples with placeholders).

    Each result has:
        - 'header':  the nearest preceding numbered heading (e.g., "1. Detect Anomalies"),
                     or "" if none precedes the block
        - 'sql':     the raw SQL, with trailing semicolon and surrounding whitespace stripped

    Args:
        md_path: Path to the walkthrough markdown file

    Returns:
        List of {"header": str, "sql": str} dicts
    """
    text = md_path.read_text()

    # Find every numbered heading and every ```sql ... ``` block (not no-parse), with positions
    heading_re = re.compile(r"^#{1,4}\s+(\d+\.[^\n]+)$", re.MULTILINE)
    sql_block_re = re.compile(r"^```sql(?!\s+no-parse)\s*\n(.*?)^```", re.MULTILINE | re.DOTALL)

    headings = [(m.start(), m.group(1).strip()) for m in heading_re.finditer(text)]

    blocks: list[dict] = []
    for m in sql_block_re.finditer(text):
        # Find the most recent heading at or before this block
        block_pos = m.start()
        header = ""
        for h_pos, h_text in headings:
            if h_pos <= block_pos:
                header = h_text
            else:
                break

        sql = m.group(1).strip().rstrip(";").strip()
        if sql:
            blocks.append({"header": header, "sql": sql})

    return blocks


_DDL_PREFIXES = (
    "CREATE TABLE",
    "CREATE OR ALTER MATERIALIZED TABLE",
    "CREATE MATERIALIZED TABLE",
    "CREATE OR REPLACE MATERIALIZED TABLE",
    "CREATE AGENT",
    "CREATE TOOL",
    "CREATE MODEL",
    "INSERT INTO",
)


def is_executable_pipeline_sql(sql: str) -> bool:
    """True if this SQL creates or modifies a pipeline (vs. an exploratory SELECT).

    Pipeline tests execute these; exploratory `SELECT ... FROM table` queries
    are skipped because they're for the user to manually inspect data.
    """
    head = sql.lstrip().upper()
    return any(head.startswith(prefix) for prefix in _DDL_PREFIXES)


_OBJECT_RE = re.compile(
    r"CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?(MATERIALIZED\s+TABLE|TABLE|AGENT|TOOL|MODEL)"
    r"\s+(?:IF\s+NOT\s+EXISTS\s+)?([`'\"]?[\w.]+[`'\"]?)",
    re.IGNORECASE,
)


def extract_sql_object(sql: str) -> tuple[str, str] | None:
    """Return (object_type, unqualified_name) for a CREATE DDL statement, or None.

    object_type is normalized to one of: "TABLE", "AGENT", "TOOL", "MODEL".
    (MATERIALIZED TABLE is normalized to "TABLE" since DESCRIBE and DROP treat them the same.)

    Examples:
        "CREATE OR ALTER MATERIALIZED TABLE foo AS …" -> ("TABLE", "foo")
        "CREATE TABLE `bar` AS …"                    -> ("TABLE", "bar")
        "CREATE AGENT my_agent WITH …"               -> ("AGENT", "my_agent")
        "INSERT INTO foo SELECT …"                   -> None
    """
    m = _OBJECT_RE.match(sql.strip())
    if not m:
        return None
    raw_type = m.group(1).upper()
    obj_type = "TABLE" if "TABLE" in raw_type else raw_type
    raw_name = m.group(2).strip("`'\"")
    name = raw_name.split(".")[-1]
    return obj_type, name
