#!/usr/bin/env python3
"""
Simple deployment script for Confluent ML Functions quickstart.
Uses credentials from credentials.env or credentials.json and deploys via Terraform.

IMPORTANT: Interactive mode always uses hardcoded regions:
- AWS: us-east-1
- Azure: eastus2

Testing mode (--testing flag) respects the region in credentials.json,
allowing developers to override the default regions if needed.
"""

import argparse
import os
import sys

from dotenv import dotenv_values, set_key

from scripts.common.credentials import (
    load_or_create_credentials_file,
    load_credentials_json,
    generate_confluent_api_keys
)
from scripts.common.login_checks import check_confluent_login
from scripts.common.terraform import get_project_root, run_terraform_output
from scripts.common.terraform_runner import run_terraform
from scripts.common.tfvars import write_tfvars_for_deployment
from scripts.common.ui import prompt_choice, prompt_with_default

# Valid cloud regions
AWS_REGIONS = [
    "us-east-1", "us-west-2", "sa-east-1",
    "ap-southeast-1", "ap-southeast-2", "ap-south-1",
    "ap-east-1", "ap-northeast-1", "ap-northeast-2"
]

AZURE_REGIONS = [
    "eastus2", "westus", "canadacentral",
    "northeurope", "westeurope", "eastasia", "centralindia"
]


def main():
    """Main entry point for deploy."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Simple deployment tool for Confluent ML functions")
    parser.add_argument("--testing", action="store_true",
                       help="Non-interactive mode using credentials.json (for automated testing)")
    args = parser.parse_args()

    print("=== Simple Deployment Tool ===\n")
    if args.testing:
        print("Running in TESTING mode (non-interactive)\n")

    root = get_project_root()
    print(f"Project root: {root}")

    # TESTING MODE: Load credentials from JSON and skip all prompts
    if args.testing:
        creds = load_credentials_json(root)

        # Extract values from JSON (ensure cloud provider is lowercase)
        cloud = creds["cloud"].lower()
        region = creds["region"]
        envs_to_deploy = ["core", "lab1", "lab2", "lab3", "lab4"]

        # Build environment variables for Terraform
        env_vars = {
            "TF_VAR_confluent_cloud_api_key": creds["confluent_cloud_api_key"],
            "TF_VAR_confluent_cloud_api_secret": creds["confluent_cloud_api_secret"],
            "TF_VAR_cloud_region": region,
            "TF_VAR_cloud_provider": cloud,
        }

        print(f"✓ Credentials loaded from credentials.json")
        print(f"  Cloud: {cloud}")
        print(f"  Region: {region}")
        print(f"  Deploying: {', '.join(envs_to_deploy)}")
        print()

        # Write terraform.tfvars files
        write_tfvars_for_deployment(root, cloud, region, creds, envs_to_deploy)

        # Load into environment
        for key, value in env_vars.items():
            os.environ[key] = value

    # INTERACTIVE MODE: Original flow
    else:
        # Step 0: Check Confluent CLI login
        if not check_confluent_login():
            print("\nError: Not logged into Confluent Cloud.")
            print("Please run: confluent login")
            sys.exit(1)
        print("✓ Confluent CLI logged in")

        # Step 1: Select cloud provider
        cloud = prompt_choice("Select cloud provider:", ["aws", "azure"])

        # Step 2: Set cloud region (hardcoded for simplicity)
        region = "us-east-1" if cloud == "aws" else "eastus2"
        print(f"Using region: {region}")

        # Load credentials file
        creds_file, creds = load_or_create_credentials_file(root)

        # Step 3: Generate Confluent API keys (optional)
        generate = input("\nGenerate new Confluent Cloud API keys? (y/n): ").strip().lower()
        if generate == "y":
            api_key, api_secret = generate_confluent_api_keys()
            if api_key and api_secret:
                set_key(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
                set_key(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)
                creds["TF_VAR_confluent_cloud_api_key"] = api_key
                creds["TF_VAR_confluent_cloud_api_secret"] = api_secret

        # Step 4: Select what to deploy
        envs_to_deploy = []
        deploy_options = [
            "Lab 1",
            "Lab 2",
            "Lab 3",
            "Lab 4",
            "All Labs (1-4)"
        ]
        env_choice = prompt_choice("What would you like to deploy?", deploy_options)

        # Map user-friendly choice to deployment targets (core auto-included for labs)
        if env_choice == "Lab 1":
            envs_to_deploy = ["core", "lab1"]
        elif env_choice == "Lab 2":
            envs_to_deploy = ["core", "lab2"]
        elif env_choice == "Lab 3":
            envs_to_deploy = ["core", "lab3"]
        elif env_choice == "Lab 4":
            envs_to_deploy = ["core", "lab4"]
        elif env_choice == "All Labs (1-4)":
            envs_to_deploy = ["core", "lab1", "lab2", "lab3", "lab4"]

        # Step 5: Prompt for required credentials
        print("\n--- Credential Configuration ---")

        # Confluent credentials (always required)
        api_key = prompt_with_default("Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", ""))
        api_secret = prompt_with_default("Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""))
        set_key(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
        set_key(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

        # Set cloud region and cloud provider
        set_key(creds_file, "TF_VAR_cloud_region", region)
        set_key(creds_file, "TF_VAR_cloud_provider", cloud)

        print()

        # Step 6: Show all credentials and confirm
        print("\n--- Configuration Summary ---")
        final_creds = dotenv_values(creds_file)
        for key, value in sorted(final_creds.items()):
            if value:
                print(f"{key}: {value}")

        print(f"\nCloud: {cloud}")
        print(f"Region: {region}")
        print(f"Deploying: {', '.join(envs_to_deploy)}")

        confirm = input("\nReady to deploy? (y/n): ").strip().lower()
        if confirm != "y":
            print("Deployment cancelled.")
            sys.exit(0)

        # Step 6.5: Write terraform.tfvars files
        print()
        write_tfvars_for_deployment(root, cloud, region, final_creds, envs_to_deploy)

        # Step 7: Load credentials into environment and deploy
        for key, value in final_creds.items():
            if value:
                os.environ[key] = value

    print("\n=== Starting Deployment ===")
    for env in envs_to_deploy:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"Warning: {env_path} does not exist, skipping.")
            continue

        if not run_terraform(env_path):
            print(f"\nDeployment failed at {env}. Stopping.")
            sys.exit(1)

    print("\n✓ All deployments completed successfully!")

    # Display the environment name
    try:
        core_state_path = root / "terraform" / "core" / "terraform.tfstate"
        if core_state_path.exists():
            core_outputs = run_terraform_output(core_state_path)
            if "confluent_environment_display_name" in core_outputs:
                env_name = core_outputs["confluent_environment_display_name"]
                print(f"\nEnvironment name: {env_name}")
    except Exception as e:
        # Don't fail deployment if we can't read the environment name
        print(f"\n⚠ Could not read environment name: {e}")


if __name__ == "__main__":
        main()
