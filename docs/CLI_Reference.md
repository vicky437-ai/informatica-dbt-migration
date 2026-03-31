# CLI Reference — infa2dbt

## Overview

`infa2dbt` is the command-line interface for the Informatica PowerCenter to dbt Migration Framework. It provides end-to-end tooling for converting Informatica ETL workflows into production-ready dbt projects deployed to Snowflake.

```
infa2dbt [OPTIONS] COMMAND [ARGS]...
```

### Global Options

| Option | Description |
|--------|-------------|
| `--version` | Show version and exit |
| `--help` | Show help and exit |

---

## Commands

| Command | Description |
|---------|-------------|
| `convert` | Convert Informatica PowerCenter XML to a dbt project |
| `discover` | Discover and inventory Informatica XML files and schemas |
| `cache` | Manage the conversion output cache |
| `validate` | Validate a dbt project via `dbt compile` / `dbt run` |
| `git-push` | Commit and push the dbt project to Git |
| `deploy` | Deploy the dbt project to Snowflake |
| `version` | Show version information |

---

## `infa2dbt convert`

Convert Informatica PowerCenter XML exports to a dbt project.

```bash
infa2dbt convert --input <PATH> --output <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-i, --input` | PATH | *(required)* | Path to XML file or directory of XML files |
| `-o, --output` | PATH | *(required)* | Output directory for the generated dbt project |
| `-m, --mode` | `new\|merge` | `new` | `new`: create fresh project; `merge`: add to existing project |
| `-c, --connection` | TEXT | `None` | Snowflake connection name (from `connections.toml`) |
| `--database` | TEXT | `TPC_DI_RAW_DATA` | Snowflake database for source schema discovery |
| `--schema` | TEXT | `MOCK_SOURCES` | Snowflake schema for source tables |
| `--no-cache` | FLAG | `False` | Disable the conversion output cache |
| `--max-heal` | INT | `3` | Maximum self-healing correction rounds |

### Examples

```bash
# Convert a single XML file (new project)
infa2dbt convert -i ./input/wf_load_orders.XML -o ./output/my_project -m new

# Convert all XML files in a directory
infa2dbt convert -i ./input/ -o ./output/my_project -m new -c myconnection

# Merge a new mapping into an existing project
infa2dbt convert -i ./input/wf_load_customers.XML -o ./output/my_project -m merge
```

---

## `infa2dbt discover`

Discover and inventory Informatica XML files and source schemas.

### Subcommands

#### `infa2dbt discover xml`

Scan a directory for Informatica PowerCenter XML files.

```bash
infa2dbt discover xml --input <PATH> [OPTIONS]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-i, --input` | PATH | *(required)* | Directory to scan for XML files |
| `--recursive / --no-recursive` | FLAG | `True` | Scan subdirectories |
| `--format` | `table\|json` | `table` | Output format |

#### `infa2dbt discover schema`

Discover source table schemas from Snowflake or XML.

```bash
infa2dbt discover schema [OPTIONS]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--source` | `snowflake\|xml\|manual` | `snowflake` | Schema source |
| `--connection` | TEXT | `None` | Snowflake connection name |
| `--database` | TEXT | `None` | Snowflake database |
| `--schema` | TEXT | `None` | Snowflake schema |
| `--xml-path` | PATH | `None` | XML file path (for `--source xml`) |
| `--json-path` | PATH | `None` | JSON file path (for `--source manual`) |
| `--format` | `table\|json` | `table` | Output format |

### Examples

```bash
# Inventory all XML files
infa2dbt discover xml -i ./input/

# Discover source schema from Snowflake
infa2dbt discover schema --source snowflake \
    --connection myconnection \
    --database TPC_DI_RAW_DATA --schema MOCK_SOURCES
```

---

## `infa2dbt cache`

Manage the conversion output cache.

### Subcommands

#### `infa2dbt cache list`

List cached conversion entries.

```bash
infa2dbt cache list [--cache-dir <PATH>]
```

#### `infa2dbt cache clear`

Clear all cached entries.

```bash
infa2dbt cache clear [--cache-dir <PATH>]
```

#### `infa2dbt cache stats`

Show cache statistics.

```bash
infa2dbt cache stats [--cache-dir <PATH>]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--cache-dir` | PATH | `.infa2dbt/cache` | Path to cache directory |

---

## `infa2dbt validate`

Validate a generated dbt project by running `dbt compile` and optionally `dbt run` / `dbt test` against the target Snowflake warehouse.

```bash
infa2dbt validate --project <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-p, --project` | PATH | *(required)* | Path to the dbt project directory |
| `--profiles-dir` | PATH | project dir | Path to `profiles.yml` directory |
| `--compile-only` | FLAG | `False` | Only run `dbt compile` (skip `dbt run`) |
| `--run-tests` | FLAG | `False` | Also run `dbt test` after `dbt run` |
| `-s, --select` | TEXT | `None` | dbt model selection syntax |
| `--full-refresh` | FLAG | `False` | Pass `--full-refresh` to `dbt run` |
| `--install-deps` | FLAG | `False` | Run `dbt deps` before validation |
| `--dbt-path` | TEXT | `dbt` | Path to dbt executable |

### Examples

```bash
# Compile-only validation (fastest)
infa2dbt validate -p ./my_dbt_project --compile-only

# Full validation (compile + run)
infa2dbt validate -p ./my_dbt_project

# Validate + run tests
infa2dbt validate -p ./my_dbt_project --run-tests

# Validate a specific model
infa2dbt validate -p ./my_dbt_project -s tag:m_load_orders
```

---

## `infa2dbt git-push`

Commit and push the dbt project to a Git repository. Initializes a Git repo if needed.

```bash
infa2dbt git-push --project <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-p, --project` | PATH | *(required)* | Path to the dbt project directory |
| `--remote` | TEXT | `origin` | Git remote name |
| `-b, --branch` | TEXT | current | Branch name (creates if needed) |
| `--remote-url` | TEXT | `None` | Git remote URL (added/updated if provided) |
| `-m, --message` | TEXT | auto | Commit message (auto-generated with timestamp) |

### Examples

```bash
# Push with auto-generated commit message
infa2dbt git-push -p ./my_dbt_project \
    --remote-url https://github.com/org/repo.git

# Push to a feature branch
infa2dbt git-push -p ./my_dbt_project -b feature/new-mapping

# Push with custom message
infa2dbt git-push -p ./my_dbt_project -m "Add journals mapping"
```

---

## `infa2dbt deploy`

Deploy the dbt project to Snowflake. Supports three deployment modes.

```bash
infa2dbt deploy --project <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-p, --project` | PATH | *(required)* | Path to the local dbt project |
| `-d, --database` | TEXT | `TPC_DI_RAW_DATA` | Snowflake database |
| `-s, --schema` | TEXT | `DBT_INGEST` | Snowflake schema |
| `-n, --project-name` | TEXT | `INFORMATICA_TO_DBT` | Snowflake dbt project object name |
| `-w, --warehouse` | TEXT | `SMALL_WH` | Snowflake warehouse |
| `--connection` | TEXT | `None` | Snowflake connection name |
| `--mode` | `direct\|git\|schedule` | `direct` | Deployment mode |
| `--git-url` | TEXT | `None` | Git repo HTTPS URL (for `--mode git`) |
| `--git-repo-name` | TEXT | `None` | Snowflake GIT REPOSITORY name (for `--mode git`) |
| `--git-branch` | TEXT | `main` | Git branch (for `--mode git`) |
| `--cron` | TEXT | `0 2 * * *` | Cron schedule (for `--mode schedule`) |
| `--dry-run` | FLAG | `False` | Show SQL without executing |

### Deployment Modes

| Mode | Description |
|------|-------------|
| `direct` | Uses `snow dbt deploy` to push the project directly to Snowflake |
| `git` | Creates a Snowflake Git Repository integration and deploys from it |
| `schedule` | Creates a Snowflake TASK to run the project on a cron schedule |

### Examples

```bash
# Direct deployment (simplest)
infa2dbt deploy -p ./my_dbt_project -d MY_DB -s MY_SCHEMA

# Git-based deployment
infa2dbt deploy -p ./my_dbt_project --mode git \
    --git-url https://github.com/org/repo.git \
    --git-repo-name my_git_repo

# Schedule (daily at 2 AM)
infa2dbt deploy -p ./my_dbt_project --mode schedule \
    --cron "0 2 * * *"

# Dry-run to preview SQL (no execution)
infa2dbt deploy -p ./my_dbt_project --mode git --dry-run \
    --git-url https://github.com/org/repo.git \
    --git-repo-name my_repo
```

---

## `infa2dbt version`

Show version information.

```bash
infa2dbt version
```

---

## End-to-End Workflow

A typical migration workflow uses the commands in sequence:

```bash
# 1. Discover source inventory
infa2dbt discover xml -i ./input/
infa2dbt discover schema --source snowflake \
    --connection myconnection \
    --database TPC_DI_RAW_DATA --schema MOCK_SOURCES

# 2. Convert all mappings
infa2dbt convert -i ./input/ -o ./output/dbt_project \
    -m new -c myconnection

# 3. Validate the generated project
infa2dbt validate -p ./output/dbt_project --run-tests

# 4. Push to Git
infa2dbt git-push -p ./output/dbt_project \
    --remote-url https://github.com/org/repo.git \
    -b main -m "Initial migration"

# 5. Deploy to Snowflake
infa2dbt deploy -p ./output/dbt_project \
    -d TPC_DI_RAW_DATA -s DBT_INGEST \
    --mode direct --connection myconnection

# 6. (Optional) Schedule recurring execution
infa2dbt deploy -p ./output/dbt_project \
    --mode schedule --cron "0 2 * * *"
```
