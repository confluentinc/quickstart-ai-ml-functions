"""
Register the Confluent Cloud MCP server with Claude Code from Terraform core outputs.

After `uv run deploy`, run `uv run setup-mcp` to make this project's Confluent
Cloud cluster + Flink compute pool queryable directly from Claude Code via the
`confluent-cloud-mcp-server` (Node.js npx package `@confluentinc/mcp-confluent`).

Useful for closing the loop on test work: list topics, describe Flink statements,
inspect Schema Registry subjects, run Flink queries — without leaving the editor.

Restart Claude Code after running this script.

Ported from the quickstart-streaming-agents repo.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

from scripts.common.terraform import get_project_root, run_terraform_output

# Node ABI versions that have prebuilt @confluentinc/kafka-javascript binaries.
_KAFKA_JS_PREBUILT_ABIS = {115, 120, 127, 131, 137}
_PREFERRED_ABI = 137  # Node 24 LTS


def _get_node_abi(node_bin: str) -> int | None:
    """Return the ABI version for the given node binary, or None on failure."""
    try:
        result = subprocess.run(
            [node_bin, "-e", "process.stdout.write(process.versions.modules)"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return int(result.stdout.strip())
    except Exception:
        return None


def _candidate_npx_paths() -> list[Path]:
    """Well-known locations where Node 24 LTS npx may be installed."""
    candidates = [
        Path("/opt/homebrew/opt/node@24/bin/npx"),  # Homebrew on Apple Silicon
        Path("/usr/local/opt/node@24/bin/npx"),  # Homebrew on Intel Mac
    ]
    nvm_dir = Path.home() / ".nvm" / "versions" / "node"
    if nvm_dir.is_dir():
        for entry in sorted(nvm_dir.iterdir(), reverse=True):
            if entry.name.startswith("v24."):
                candidates.append(entry / "bin" / "npx")
    return candidates


def _resolve_npx() -> str:
    """Return the npx binary to use for the MCP server.

    Prefers Node 24 LTS for prebuilt @confluentinc/kafka-javascript binaries.
    Falls back to bare 'npx' on PATH if nothing compatible is found.
    """
    path_node_abi: int | None = None
    path_node_version = ""
    try:
        ver_result = subprocess.run(
            ["node", "--version"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        path_node_version = ver_result.stdout.strip().lstrip("v")
        path_node_abi = _get_node_abi("node")
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        pass

    if path_node_abi in _KAFKA_JS_PREBUILT_ABIS:
        label = " (Node 24 LTS)" if path_node_abi == _PREFERRED_ABI else ""
        print(f"Using Node {path_node_version}{label}")
        return "npx"

    for npx_path in _candidate_npx_paths():
        if not npx_path.exists():
            continue
        node_bin = npx_path.parent / "node"
        abi = _get_node_abi(str(node_bin))
        if abi in _KAFKA_JS_PREBUILT_ABIS:
            ver_result = subprocess.run(
                [str(node_bin), "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            version_str = ver_result.stdout.strip().lstrip("v")
            label = " (Node 24 LTS)" if abi == _PREFERRED_ABI else ""
            print(f"Using Node {version_str}{label} from {npx_path.parent}")
            return str(npx_path)

    if path_node_abi is not None:
        print(
            f"Warning: Node {path_node_version} (ABI {path_node_abi}) has no prebuilt "
            f"@confluentinc/kafka-javascript binary."
        )
        print("  npx will compile it from source the first time the MCP server starts — this can take several minutes.")
    else:
        print("Warning: 'node' not found on PATH or in well-known locations.")
    print("  To avoid the wait, install Node 24 LTS:")
    print("    With nvm:      nvm install 24 && nvm use 24")
    print("    With Homebrew: brew install node@24")
    print("  Then re-run `uv run setup-mcp`.")
    if path_node_abi is not None:
        answer = input(f"  Continue anyway with Node {path_node_version}? [y/N] ").strip().lower()
        if answer != "y":
            sys.exit(0)
    else:
        sys.exit(1)
    return "npx"


# Maps terraform output names to MCP env var names.
_TF_TO_MCP = {
    "confluent_kafka_cluster_bootstrap_endpoint": ["BOOTSTRAP_SERVERS"],
    "app_manager_kafka_api_key": ["KAFKA_API_KEY"],
    "app_manager_kafka_api_secret": ["KAFKA_API_SECRET"],
    "confluent_kafka_cluster_rest_endpoint": ["KAFKA_REST_ENDPOINT"],
    "confluent_kafka_cluster_id": ["KAFKA_CLUSTER_ID"],
    "confluent_environment_id": ["KAFKA_ENV_ID", "FLINK_ENV_ID"],
    "app_manager_flink_api_key": ["FLINK_API_KEY"],
    "app_manager_flink_api_secret": ["FLINK_API_SECRET"],
    "confluent_flink_rest_endpoint": ["FLINK_REST_ENDPOINT"],
    "confluent_flink_compute_pool_id": ["FLINK_COMPUTE_POOL_ID"],
    "confluent_organization_id": ["FLINK_ORG_ID"],
    "confluent_environment_display_name": ["FLINK_CATALOG_NAME"],
    "confluent_kafka_cluster_display_name": ["FLINK_DATABASE_NAME"],
    "app_manager_schema_registry_api_key": ["SCHEMA_REGISTRY_API_KEY"],
    "app_manager_schema_registry_api_secret": ["SCHEMA_REGISTRY_API_SECRET"],
    "confluent_schema_registry_rest_endpoint": ["SCHEMA_REGISTRY_ENDPOINT"],
    "confluent_cloud_api_key": ["CONFLUENT_CLOUD_API_KEY"],
    "confluent_cloud_api_secret": ["CONFLUENT_CLOUD_API_SECRET"],
}


def _clear_npx_cache() -> None:
    """Remove any cached @confluentinc/mcp-confluent entry from ~/.npm/_npx.

    Forces npx to re-download the package on the next MCP server start, which
    avoids stale-binary issues after Node version changes or partial installs.
    """
    npx_cache = Path.home() / ".npm" / "_npx"
    if not npx_cache.exists():
        return
    for entry in npx_cache.iterdir():
        if not entry.is_dir():
            continue
        if (entry / "node_modules" / "@confluentinc" / "mcp-confluent").exists():
            print(f"  Clearing npx cache entry: {entry.name}")
            shutil.rmtree(entry)


def main() -> None:
    npx_bin = _resolve_npx()
    _clear_npx_cache()

    project_root = get_project_root()
    state_path = project_root / "terraform" / "core" / "terraform.tfstate"

    if not state_path.exists():
        print("Error: terraform/core/terraform.tfstate not found. Run `uv run deploy` first.")
        sys.exit(1)

    core_outputs = run_terraform_output(state_path)

    env_vars: dict[str, str] = {}
    for tf_key, mcp_vars in _TF_TO_MCP.items():
        value = core_outputs.get(tf_key, "") or ""
        for var in mcp_vars:
            # Terraform emits BOOTSTRAP_SERVERS as "SASL_SSL://host:port" but the
            # MCP server requires bare "host:port".
            if var == "BOOTSTRAP_SERVERS" and isinstance(value, str) and "://" in value:
                env_vars[var] = value.split("://", 1)[1]
            else:
                env_vars[var] = value

    # Write MCP config directly to ~/.claude.json (mirrors `claude mcp add --scope local`).
    claude_json_path = Path.home() / ".claude.json"
    if claude_json_path.exists():
        with claude_json_path.open() as f:
            claude_data = json.load(f)
    else:
        claude_data = {}

    project_key = str(project_root)
    (claude_data.setdefault("projects", {}).setdefault(project_key, {}).setdefault("mcpServers", {}))[
        "confluent-cloud-mcp-server"
    ] = {
        "command": npx_bin,
        "args": ["-y", "@confluentinc/mcp-confluent"],
        "env": env_vars,
    }

    with claude_json_path.open("w") as f:
        json.dump(claude_data, f, indent=2)
        f.write("\n")

    print("✓ Confluent MCP server registered as 'confluent-cloud-mcp-server' (local scope)")
    print(f"  Project: {project_key}")
    print("  Restart Claude Code to activate.")


if __name__ == "__main__":
    main()
