"""
Terraform variables file (terraform.tfvars) management utilities.

Provides functions for:
- Writing terraform.tfvars files with automatic backup
- Generating formatted tfvars content for core and lab modules
- Orchestrating tfvars file creation across multiple environments
"""

import shutil
from pathlib import Path
from typing import Dict, Optional


def get_credential_value(creds: Dict[str, str], key: str) -> Optional[str]:
    """
    Get credential value, checking both TF_VAR_ prefixed and non-prefixed keys.

    Args:
        creds: Dictionary of credentials
        key: Key to look up (without TF_VAR_ prefix)

    Returns:
        Value if found, None otherwise
    """
    return creds.get(key) or creds.get(f"TF_VAR_{key}")


def write_tfvars_file(tfvars_path: Path, content: str) -> bool:
    """
    Write terraform.tfvars file with backup of existing file.

    Args:
        tfvars_path: Path to terraform.tfvars file
        content: Content to write

    Returns:
        True if successful, False otherwise
    """
    try:
        # Backup existing file
        if tfvars_path.exists():
            backup_path = tfvars_path.with_suffix(".tfvars.backup")
            shutil.copy2(tfvars_path, backup_path)

        # Ensure parent directory exists
        tfvars_path.parent.mkdir(parents=True, exist_ok=True)

        # Write new content
        with open(tfvars_path, 'w') as f:
            f.write(content)

        return True
    except Exception as e:
        print(f"Error writing {tfvars_path}: {e}")
        return False


def generate_core_tfvars_content(
    cloud: str,
    region: str,
    api_key: str,
    api_secret: str,
    cloud_provider: Optional[str] = None,
) -> str:
    """
    Generate terraform.tfvars content for Core module.

    Args:
        cloud: Cloud provider (aws or azure)
        region: Cloud region
        api_key: Confluent Cloud API key
        api_secret: Confluent Cloud API secret
        cloud_provider: Cloud provider value for terraform variable (defaults to cloud)

    Returns:
        Formatted terraform.tfvars content
    """
    # Ensure cloud provider is lowercase for terraform validation
    provider = (cloud_provider or cloud).lower()
    content = f"""# Core Infrastructure Configuration
cloud_region = "{region}"
confluent_cloud_api_key = "{api_key}"
confluent_cloud_api_secret = "{api_secret}"
cloud_provider = "{provider}"
"""

    return content


def write_tfvars_for_deployment(
    root: Path,
    cloud: str,
    region: str,
    creds: Dict[str, str],
    envs_to_deploy: list
) -> None:
    """
    Write terraform.tfvars files for all environments being deployed.

    Args:
        root: Project root directory
        cloud: Cloud provider (aws or azure)
        region: Cloud region
        creds: Credentials dictionary (supports both TF_VAR_ prefixed and non-prefixed keys)
        envs_to_deploy: List of environments to deploy (core, lab1, lab2, lab3, lab4)
    """
    # Core terraform.tfvars
    if "core" in envs_to_deploy:
        api_key = get_credential_value(creds, "confluent_cloud_api_key")
        api_secret = get_credential_value(creds, "confluent_cloud_api_secret")
        if api_key and api_secret:
            core_tfvars_path = root / "terraform" / "core" / "terraform.tfvars"
            content = generate_core_tfvars_content(
                cloud, region, api_key, api_secret,
                cloud_provider=cloud,
            )
            if write_tfvars_file(core_tfvars_path, content):
                print(f"✓ Wrote {core_tfvars_path}")

    # Lab terraform.tfvars (labs have no variables, write empty tfvars)
    for lab in ["lab1", "lab2", "lab3", "lab4"]:
        if lab in envs_to_deploy:
            lab_tfvars_path = root / "terraform" / lab / "terraform.tfvars"
            if write_tfvars_file(lab_tfvars_path, "# No lab-specific variables\n"):
                print(f"✓ Wrote {lab_tfvars_path}")
