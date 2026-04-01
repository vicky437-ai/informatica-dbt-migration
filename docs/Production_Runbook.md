# Production Runbook — infa2dbt Framework

## Purpose

This runbook provides step-by-step instructions for setting up and running the `infa2dbt` migration framework on a new machine. Follow this guide to go from a fresh environment to a fully executed Informatica-to-dbt migration pipeline.

---

## Prerequisites

### Software Requirements

| Software | Minimum Version | Purpose |
|----------|----------------|---------|
| Python | 3.9+ | Runtime for infa2dbt CLI |
| pip | 21.0+ | Python package installer (ships with Python) |
| Git | 2.30+ | Version control, used by `git-push` command |
| Snowflake CLI (`snow`) | 3.0+ | Deploy and execute dbt projects natively on Snowflake |
| dbt-core + dbt-snowflake | 1.7+ | Local dbt compilation and validation |

### Snowflake Requirements

| Requirement | Details |
|-------------|---------|
| Snowflake account | Active account with `ACCOUNTADMIN` or equivalent role |
| Warehouse | A running warehouse (e.g. `SMALL_WH`) |
| Database | Target database for dbt models (e.g. `MY_DATABASE`) |
| Source schema | Schema containing source tables that Informatica mappings reference |
| Target schema | Schema where dbt models will be materialized (e.g. `INFORMATICA_TO_DBT`) |
| Deploy schema | Schema for the Snowflake-native dbt project object (e.g. `DBT_INGEST`) |
| Cortex LLM access | The `convert` command uses `SNOWFLAKE.CORTEX.COMPLETE()` for code generation |

### Input Files

| Item | Details |
|------|---------|
| Informatica XML exports | PowerCenter XML files exported from the Informatica repository (workflows, sessions, or mappings) |
| Source table data | The Snowflake source schema must contain tables matching the sources defined in the Informatica mappings |

---

## Step 1: Install Python and System Tools

### macOS

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python 3.9+
brew install python@3.12

# Install Git (if not installed)
brew install git

# Verify
python3 --version   # Must be 3.9+
git --version        # Must be 2.30+
```

### Windows

```powershell
# Download and install Python from https://www.python.org/downloads/
# Ensure "Add Python to PATH" is checked during installation

# Install Git from https://git-scm.com/download/win

# Verify (from PowerShell or Command Prompt)
python --version
git --version
```

### Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv git

# Verify
python3 --version
git --version
```

---

## Step 2: Install Snowflake CLI

```bash
# Install via pip
pip install snowflake-cli

# Verify
snow --version
```

---

## Step 3: Configure Snowflake Connection

Create or edit `~/.snowflake/config.toml`:

```toml
default_connection_name = "myconnection"

[connections]
[connections.myconnection]
user = "<YOUR_USERNAME>"
account = "<YOUR_ACCOUNT_IDENTIFIER>"
password = "<YOUR_PASSWORD>"
authenticator = "snowflake"
warehouse = "<YOUR_WAREHOUSE>"
database = "<YOUR_DATABASE>"
schema = "<YOUR_SCHEMA>"
role = "<YOUR_ROLE>"
connection_timeout = 60
```

> **Security note:** For production use, consider `authenticator = "externalbrowser"` or key-pair authentication instead of storing a password in plaintext.

Verify the connection:

```bash
snow connection test -c myconnection
```

---

## Step 4: Clone and Install the Framework

```bash
# Clone the repository
git clone <REPOSITORY_URL>
cd informatica-to-dbt

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

# Install the framework and all dependencies
pip install -e .
```

This installs the following Python dependencies (from `pyproject.toml`):

| Package | Version | Purpose |
|---------|---------|---------|
| lxml | >=4.9 | XML parsing of Informatica PowerCenter exports |
| networkx | >=3.0 | DAG construction for transformation dependency graphs |
| sqlparse | >=0.4 | SQL parsing and formatting |
| jinja2 | >=3.1 | dbt model template rendering |
| pyyaml | >=6.0 | YAML generation for dbt schema and source files |
| click | >=8.1 | CLI framework |

Verify installation:

```bash
infa2dbt version
```

Expected output:
```
infa2dbt v1.0.0
```

---

## Step 5: Install dbt

```bash
pip install dbt-core dbt-snowflake

# Verify
dbt --version
```

---

## Step 6: Prepare Input Files

Place your Informatica PowerCenter XML exports into an input directory:

```
project_root/
├── input/
│   ├── wf_mapping_one.XML
│   ├── wf_mapping_two.XML
│   └── s_m_mapping_three.XML
└── ...
```

> XML files can be workflow exports (`wf_*.XML`), session exports (`s_m_*.XML`), or mapping-only exports. The framework auto-detects the type.

---

## Step 7: Run the Migration Pipeline

Execute each step in order. All commands are run from the project root directory.

### 7.1 Convert

Parse Informatica XML files and generate a dbt project:

```bash
infa2dbt convert \
    -i ./input/ \
    -o ./output/dbt_project \
    -m new \
    --connection myconnection \
    --source-schema <YOUR_SOURCE_SCHEMA> \
    --log-level info
```

**What this does:**
- Parses all XML files in `./input/`
- Extracts mappings, transformations, source/target definitions
- Calls Snowflake Cortex LLM to generate dbt SQL models
- Produces a complete dbt project in `./output/dbt_project/`

**Expected output:** A dbt project directory containing `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`, `seeds/`, `snapshots/`, and `tests/`.

### 7.2 Discover

Inventory the XML files and discover source schemas:

```bash
infa2dbt discover \
    -i ./input/ \
    --schema-source snowflake \
    --database <YOUR_DATABASE> \
    --schema <YOUR_SOURCE_SCHEMA>
```

**What this does:**
- Scans the input directory for all XML files
- Reports mapping count, source tables, and target tables
- Queries Snowflake `INFORMATION_SCHEMA` to discover column definitions

### 7.3 Report

Generate an EWI (Errors/Warnings/Informational) assessment report:

```bash
infa2dbt report \
    -p ./output/dbt_project \
    -f both
```

**What this does:**
- Reads conversion metrics saved during the `convert` step
- Generates an HTML report and JSON report in `./output/dbt_project/reports/`

### 7.4 Validate

Compile, run models, and execute tests:

```bash
infa2dbt validate \
    -p ./output/dbt_project \
    --run-tests
```

**What this does:**
- Runs `dbt compile` to check SQL syntax
- Runs `dbt run` to materialize all models in Snowflake
- Runs `dbt test` to execute all schema and data tests

**Success criteria:** All models pass, all tests pass, zero errors.

### 7.5 Deploy

Deploy the dbt project to Snowflake as a native dbt project object:

```bash
infa2dbt deploy \
    -p ./output/dbt_project \
    -d <YOUR_DATABASE> \
    -s <YOUR_DEPLOY_SCHEMA> \
    -n <YOUR_PROJECT_NAME> \
    --connection myconnection \
    --mode direct
```

**What this does:**
- Packages the dbt project
- Uses `snow dbt deploy` to push it as a Snowflake-native dbt project object

### 7.6 Execute on Snowflake

Run models and tests natively on Snowflake:

```bash
# Run all models
snow dbt execute -c myconnection \
    --database <YOUR_DATABASE> \
    --schema <YOUR_DEPLOY_SCHEMA> \
    <YOUR_PROJECT_NAME> run

# Run all tests
snow dbt execute -c myconnection \
    --database <YOUR_DATABASE> \
    --schema <YOUR_DEPLOY_SCHEMA> \
    <YOUR_PROJECT_NAME> test
```

### 7.7 Push to Git

Commit and push the generated dbt project to a Git repository:

```bash
infa2dbt git-push \
    -p ./output/dbt_project \
    --remote-url <YOUR_GIT_REPO_URL> \
    -b main \
    -m "Informatica to dbt migration"
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| `ModuleNotFoundError: informatica_to_dbt` | Framework not installed | Run `pip install -e .` from the project root |
| `snow: command not found` | Snowflake CLI not installed | Run `pip install snowflake-cli` |
| `dbt: command not found` | dbt not installed | Run `pip install dbt-core dbt-snowflake` |
| `Connection failed` | Bad credentials in `config.toml` | Verify account, user, password; run `snow connection test` |
| `TRY_CAST error (001065)` | Source column already the target type | The framework handles this automatically; if seen in custom models, use `TO_CHAR()` directly |
| `Cortex LLM timeout` | Large mapping or slow LLM response | Retry the `convert` command; cached results are reused automatically |
| `dbt test failures` | Source data quality issues | Review test definitions in `_stg__schema.yml`; adjust accepted values or thresholds |
| `Permission denied on deploy` | Insufficient Snowflake role | Ensure the configured role has `CREATE DATABASE`, `CREATE SCHEMA`, and `USAGE` grants |

### Verifying a Successful Run

After completing all steps, verify:

```bash
# Check model count and test results
infa2dbt validate -p ./output/dbt_project --run-tests

# Query a mart table in Snowflake
snow sql -c myconnection -q "SELECT COUNT(*) FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.<MART_TABLE>"
```

### Cache Management

If you need to re-run conversion from scratch (e.g. after updating XML files):

```bash
# Clear the LLM response cache
infa2dbt cache clear --yes

# Re-run convert
infa2dbt convert -i ./input/ -o ./output/dbt_project -m new \
    --connection myconnection --source-schema <YOUR_SOURCE_SCHEMA>
```

---

## Directory Structure After a Complete Run

```
project_root/
├── input/                          # Informatica XML exports (your input)
│   ├── wf_mapping_one.XML
│   └── ...
├── output/
│   └── dbt_project/                # Generated dbt project (your output)
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── macros/
│       ├── models/
│       │   └── <mapping_name>/
│       │       ├── staging/        # Source views + schema tests
│       │       ├── intermediate/   # Transformation logic
│       │       └── marts/          # Final business tables
│       ├── reports/                # EWI assessment reports
│       ├── seeds/
│       ├── snapshots/
│       └── tests/
├── pyproject.toml
└── src/
    └── informatica_to_dbt/         # Framework source code
```

---

## Quick Reference — Full Pipeline (Copy-Paste)

Replace all `<PLACEHOLDER>` values with your environment-specific settings:

```bash
# Activate virtual environment
source .venv/bin/activate

# 1. Convert
infa2dbt convert -i ./input/ -o ./output/dbt_project -m new \
    --connection <CONNECTION> --source-schema <SOURCE_SCHEMA>

# 2. Discover
infa2dbt discover -i ./input/ --schema-source snowflake \
    --database <DATABASE> --schema <SOURCE_SCHEMA>

# 3. Report
infa2dbt report -p ./output/dbt_project -f both

# 4. Validate
infa2dbt validate -p ./output/dbt_project --run-tests

# 5. Deploy
infa2dbt deploy -p ./output/dbt_project -d <DATABASE> -s <DEPLOY_SCHEMA> \
    -n <PROJECT_NAME> --connection <CONNECTION> --mode direct

# 6. Execute
snow dbt execute -c <CONNECTION> --database <DATABASE> --schema <DEPLOY_SCHEMA> <PROJECT_NAME> run
snow dbt execute -c <CONNECTION> --database <DATABASE> --schema <DEPLOY_SCHEMA> <PROJECT_NAME> test

# 7. Git Push
infa2dbt git-push -p ./output/dbt_project --remote-url <GIT_REPO_URL> -b main -m "Migration complete"
```
