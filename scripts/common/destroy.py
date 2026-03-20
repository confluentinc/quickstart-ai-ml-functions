#!/usr/bin/env python3
"""
Simple destruction script for Confluent ML Functions quickstart.
Uses credentials from credentials.env for destruction via Terraform.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

from .credentials import load_or_create_credentials_file
from .terraform import get_project_root
from .terraform_runner import run_terraform_destroy
from .ui import prompt_choice


def cleanup_terraform_artifacts(env_path: Path) -> None:
    """
    Remove all terraform artifacts from a directory after successful destroy.

    Removes:
    - *.tfstate* files
    - *.tfvars* files
    - .terraform/ directory
    - .terraform.lock.hcl file
    - FLINK_SQL_COMMANDS.md (auto-generated summary)
    - mcp_commands.txt (legacy file)

    Does NOT remove credentials.env (which is in project root, not env directories).

    Args:
        env_path: Path to terraform environment directory
    """
    try:
        # Remove generated state and variable files
        for pattern in ("*.tfstate*", "*.tfvars*"):
            for f in env_path.glob(pattern):
                f.unlink()

        # Remove .terraform directory
        terraform_dir = env_path / ".terraform"
        if terraform_dir.exists():
            shutil.rmtree(terraform_dir)

        # Remove individual generated files
        for name in (".terraform.lock.hcl", "FLINK_SQL_COMMANDS.md", "mcp_commands.txt"):
            path = env_path / name
            if path.exists():
                path.unlink()

    except Exception:
        # Silently continue if cleanup fails - destroy was successful
        pass


def main():
    """Main entry point for destroy."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Destroy deployed Confluent ML functions resources")
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Non-interactive mode using credentials.env (for automated testing)",
    )
    args = parser.parse_args()

    print("=== Simple Destroy Tool ===\n")
    if args.testing:
        print("Running in TESTING mode (non-interactive)\n")

    root = get_project_root()
    print(f"Project root: {root}")

    # TESTING MODE: Load from credentials.env and skip prompts
    if args.testing:
        _, raw_creds = load_or_create_credentials_file(root)
        cloud = raw_creds.get("TF_VAR_cloud_provider", "").lower()
        envs_to_destroy = ["lab4", "lab3", "lab2", "lab1", "core"]  # Reverse order

        # Build environment variables
        env_vars = {
            "TF_VAR_confluent_cloud_api_key": raw_creds.get("TF_VAR_confluent_cloud_api_key", ""),
            "TF_VAR_confluent_cloud_api_secret": raw_creds.get("TF_VAR_confluent_cloud_api_secret", ""),
            "TF_VAR_cloud_region": raw_creds.get("TF_VAR_cloud_region", ""),
            "TF_VAR_cloud_provider": cloud,
        }

        # Load into environment
        for key, value in env_vars.items():
            os.environ[key] = value

        print("✓ Destroying all resources")
        print(f"  Cloud: {cloud}")
        print(f"  Environments: {', '.join(envs_to_destroy)}")
        print()

    # INTERACTIVE MODE: Original flow
    else:
        # Step 1: Select cloud provider
        cloud = prompt_choice("Select cloud provider to destroy:", ["aws", "azure"])

        # Step 2: Always destroy all environments
        envs_to_destroy = ["lab4", "lab3", "lab2", "lab1", "core"]
        print(f"✓ Will destroy all environments: {', '.join(envs_to_destroy)}")

        # Load credentials file
        _creds_file, creds = load_or_create_credentials_file(root)

        # Step 3: Load credentials into environment
        for key, value in creds.items():
            if value:
                os.environ[key] = value

        # Step 4: Show summary and confirm
        print("\n--- Destroy Summary ---")
        print(f"Cloud: {cloud}")
        print(f"Destroying: {', '.join(envs_to_destroy)}")
        print("\n⚠️  WARNING: This will permanently destroy all resources in the selected environments!")

        confirm = input("\nAre you sure you want to proceed? (y/n): ").strip().lower()
        if confirm != "y":
            print("Destroy cancelled.")
            sys.exit(0)

    # Step 5: Destroy environments
    print("\n=== Starting Destroy ===")
    for env in envs_to_destroy:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"⊘ Skipping {env}: directory does not exist")
            continue

        # Check if terraform state exists (indicates it was deployed)
        state_file = env_path / "terraform.tfstate"
        if not state_file.exists():
            print(f"⊘ Skipping {env}: no terraform state found (never deployed)")
            continue

        print(f"\n→ Destroying {env}...")
        if run_terraform_destroy(env_path):
            # Cleanup terraform artifacts after successful destroy
            cleanup_terraform_artifacts(env_path)
        else:
            print(f"\n✗ Destroy failed at {env}. Continuing with remaining environments...")

    print("\n✓ Destroy process completed!")


if __name__ == "__main__":
    main()
