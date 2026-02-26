# ML Functions on Confluent Cloud Quickstart

[![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-ai-ml-functions&utm_source=github&utm_medium=demo)

Build real-time ML pipelines with [Confluent Cloud](https://www.confluent.io/confluent-cloud/). This quickstart provisions core Confluent infrastructure (Kafka, Flink, Schema Registry) and includes placeholder lab folders ready to be populated with ML function exercises.

## Prerequisites

**Required accounts & credentials:**

- [![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-ai-ml-functions&utm_source=github&utm_medium=demo)

**Required tools:**

- **[Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html)** - must be logged in
- **[Git](https://github.com/git/git)**
- **[Terraform](https://github.com/hashicorp/terraform)**
- **[uv](https://github.com/astral-sh/uv)**

<details>
<summary> Installation commands (Mac/Windows)</summary>
**Mac:**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```
</details>

## Quick Start

**1. Clone the repository and navigate to the Quickstart directory:**

```bash
git clone https://github.com/confluentinc/quickstart-ai-ml-functions.git
cd quickstart-ai-ml-functions
```

**2. One command deployment:**

```bash
uv run deploy
```

That's it! The script will guide you through setup and deployment of your chosen lab(s).

## Directory Structure

```
quickstart-ai-ml-functions/
├── terraform/
│   ├── core/              # Shared Confluent Cloud infra for all labs
│   ├── lab1/              # Lab 1 resources
│   ├── lab2/              # Lab 2 resources
│   ├── lab3/              # Lab 3 resources
│   └── lab4/              # Lab 4 resources
├── deploy.py              # Start here with uv run deploy
└── scripts/               # Python utilities invoked with uv
```

## Cleanup

```bash
# Automated
uv run destroy
```

## Sign up for early access to Flink AI features

For early access to exciting new Flink AI features, [fill out this form and we'll add you to our early access previews.](https://events.confluent.io/early-access-flink-features)
