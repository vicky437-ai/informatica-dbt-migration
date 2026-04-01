# CLI Reference — infa2dbt

## Overview

`infa2dbt` is the command-line interface for the Informatica PowerCenter to dbt Migration Framework. It provides end-to-end tooling for converting any Informatica ETL workflow into a production-ready dbt project deployed to Snowflake.

```
infa2dbt [OPTIONS] COMMAND [ARGS]...
```

> **Note:** If `infa2dbt` is not installed as a global command, use `python -m informatica_to_dbt.cli` instead.

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
| `discover` | Discover and inventory Informatica XML files and source schemas |
| `report` | Generate an EWI assessment report from conversion metrics |
| `cache` | Manage the conversion output cache |
| `validate` | Validate a dbt project via `dbt compile` / `dbt run` / `dbt test` |
| `deploy` | Deploy the dbt project to Snowflake |
| `git-push` | Commit and push the dbt project to Git |
| `version` | Show version information |

---

## `infa2dbt convert`

Convert Informatica PowerCenter XML exports to a dbt project. Parses XML workflows and mappings, uses Snowflake Cortex LLM to generate dbt models, validates the output, and assembles a unified dbt project.

```bash
infa2dbt convert --input <PATH> --output <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-i, --input` | PATH | *(required)* | Path to an Informatica XML file or directory of XML files |
| `-o, --output` | PATH | *(required)* | Target dbt project directory (created if it doesn't exist) |
| `-m, --mode` | `new\|merge` | `new` | `new`: create fresh project; `merge`: add into an existing project |
| `-n, --project-name` | TEXT | `informatica_dbt` | dbt project name |
| `--model` | TEXT | *(auto)* | Snowflake Cortex LLM model for code generation |
| `--connection` | TEXT | `None` | Snowflake connection name (from `~/.snowflake/config.toml`) |
| `--log-level` | `debug\|info\|warning\|error` | `info` | Logging verbosity |
| `--no-cache` | FLAG | `False` | Disable output caching (always regenerate) |
| `--clear-cache` | FLAG | `False` | Clear all cached conversions before running |
| `--source-schema` | TEXT | `None` | Override source schema in all `_sources.yml` (e.g. `MOCK_SOURCES`) |

### Examples

```bash
# Convert a single XML file (new project)
infa2dbt convert -i ./input/wf_load_orders.XML -o ./output/my_project -m new

# Convert all XML files in a directory with Snowflake connection
infa2dbt convert -i ./input/ -o ./output/my_project -m new \
    --connection myconnection --source-schema RAW_SOURCES

# Merge a new mapping into an existing project
infa2dbt convert -i ./input/wf_load_customers.XML -o ./output/my_project -m merge

# Convert with debug logging and cache disabled
infa2dbt convert -i ./input/ -o ./output/my_project -m new \
    --connection myconnection --log-level debug --no-cache
```

---

## `infa2dbt discover`

Discover and inventory Informatica XML files and source table schemas. Scans a directory for Informatica PowerCenter XML files, reports metadata (mapping count, sources, targets), and optionally discovers source table schemas from Snowflake, the XML itself, or a JSON file.

```bash
infa2dbt discover --input <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-i, --input` | PATH | *(required)* | Path to an Informatica XML file or directory of XML files |
| `--schema-source` | `xml\|snowflake\|json` | *(none)* | Schema discovery mode: `xml` from parsed XML, `snowflake` from `INFORMATION_SCHEMA`, `json` from file |
| `--database` | TEXT | `None` | Snowflake database name (required for `--schema-source snowflake`) |
| `--schema` | TEXT | `None` | Snowflake schema name (required for `--schema-source snowflake`) |
| `--json-path` | PATH | `None` | Path to `source_map.json` (required for `--schema-source json`) |
| `-o, --output` | PATH | `None` | Save discovered schema to a JSON file |

### Examples

```bash
# Scan a directory and show XML inventory
infa2dbt discover -i ./input/

# Discover schemas from Snowflake and save to JSON
infa2dbt discover -i ./input/ \
    --schema-source snowflake \
    --database MY_DB --schema RAW_SOURCES \
    -o source_map.json

# Discover schemas from parsed XML metadata
infa2dbt discover -i ./input/ --schema-source xml -o source_map.json

# Discover schemas from an existing JSON file
infa2dbt discover -i ./input/ --schema-source json --json-path ./source_map.json
```

---

## `infa2dbt report`

Generate an EWI (Errors/Warnings/Informational) assessment report from saved conversion metrics. The `convert` command automatically saves metrics to `<project-dir>/.infa2dbt/last_metrics.json` after each run.

```bash
infa2dbt report [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-p, --project-dir` | PATH | *(none)* | dbt project directory (used to locate default metrics file) |
| `-m, --metrics-file` | PATH | `<project-dir>/.infa2dbt/last_metrics.json` | Path to a metrics JSON file produced by `convert` |
| `-o, --output` | PATH | `<project-dir>/reports` | Output directory for generated reports |
| `-f, --format` | `html\|json\|both` | `both` | Report format to generate |

### Examples

```bash
# Generate both HTML and JSON reports (default)
infa2dbt report -p ./my_dbt_project -f both

# Generate HTML report only
infa2dbt report -p ./my_dbt_project -f html

# Generate from a specific metrics file to a custom directory
infa2dbt report -m ./metrics/conversion_metrics.json -o ./reports/ -f both
```

---

## `infa2dbt cache`

Manage the conversion output cache. The cache ensures that re-running `convert` on the same XML input produces the same dbt output without making additional LLM calls.

### Subcommands

#### `infa2dbt cache list`

List all cached conversion entries.

```bash
infa2dbt cache list [--cache-dir <PATH>]
```

#### `infa2dbt cache clear`

Clear all cached conversion entries.

```bash
infa2dbt cache clear [--cache-dir <PATH>] [--yes]
```

#### `infa2dbt cache stats`

Show cache statistics (total entries, disk usage).

```bash
infa2dbt cache stats [--cache-dir <PATH>]
```

### Options (shared across subcommands)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--cache-dir` | TEXT | `.infa2dbt/cache` | Path to cache directory |
| `--yes` | FLAG | `False` | Confirm `clear` action without prompting (only for `cache clear`) |

### Examples

```bash
# List cached entries
infa2dbt cache list

# Show cache statistics
infa2dbt cache stats

# Clear all cached entries (with confirmation prompt)
infa2dbt cache clear

# Clear without prompting
infa2dbt cache clear --yes
```

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
| `-s, --select` | TEXT | `None` | dbt model selection syntax (e.g. `tag:<mapping_name>`) |
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

# Validate a specific mapping by tag
infa2dbt validate -p ./my_dbt_project -s tag:m_load_orders

# Full refresh with dependency installation
infa2dbt validate -p ./my_dbt_project --run-tests --full-refresh --install-deps
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
| `-p, --project` | PATH | *(required)* | Path to the local dbt project directory |
| `-d, --database` | TEXT | *(from config)* | Snowflake database for the dbt project object |
| `-s, --schema` | TEXT | *(from config)* | Snowflake schema for the dbt project object |
| `-n, --project-name` | TEXT | *(auto)* | Name for the Snowflake dbt project object |
| `-w, --warehouse` | TEXT | *(from config)* | Snowflake warehouse for execution |
| `--connection` | TEXT | `None` | Snowflake connection name |
| `--mode` | `direct\|git\|schedule` | `direct` | Deployment mode |
| `--git-url` | TEXT | `None` | Git repository HTTPS URL (required for `--mode git`) |
| `--git-repo-name` | TEXT | `None` | Snowflake GIT REPOSITORY object name (for `--mode git`) |
| `--git-branch` | TEXT | `main` | Git branch to deploy from (for `--mode git`) |
| `--cron` | TEXT | `0 2 * * *` | Cron schedule (for `--mode schedule`) |
| `--dry-run` | FLAG | `False` | Show generated SQL without executing (for `git`/`schedule` modes) |

### Deployment Modes

| Mode | Description |
|------|-------------|
| `direct` | Uses `snow dbt deploy` to push the project directly to Snowflake |
| `git` | Creates a Snowflake Git Repository integration and deploys from it |
| `schedule` | Creates a Snowflake TASK to run the project on a cron schedule |

### Examples

```bash
# Direct deployment (simplest)
infa2dbt deploy -p ./my_dbt_project -d MY_DB -s MY_SCHEMA \
    --connection myconnection

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

## `infa2dbt git-push`

Commit and push the dbt project to a Git repository. Initializes a Git repo if needed, stages all changes, commits, and pushes to the specified remote and branch.

```bash
infa2dbt git-push --project <PATH> [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-p, --project` | PATH | *(required)* | Path to the dbt project directory |
| `--remote` | TEXT | `origin` | Git remote name |
| `-b, --branch` | TEXT | current branch | Branch name to push to (creates if needed) |
| `--remote-url` | TEXT | `None` | Git remote URL (added/updated if provided) |
| `-m, --message` | TEXT | auto-generated | Commit message (auto-generated with timestamp if not provided) |

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

## `infa2dbt version`

Show version information.

```bash
infa2dbt version
```

Output:
```
infa2dbt v1.0.0
Informatica PowerCenter XML to dbt project converter
Powered by Snowflake Cortex LLM
```

---

## End-to-End Workflow

A typical migration workflow uses the commands in sequence:

```bash
# 1. Convert all Informatica XML mappings to a dbt project
infa2dbt convert -i ./input/ -o ./output/dbt_project \
    -m new --connection myconnection --source-schema RAW_SOURCES

# 2. Discover source inventory and schemas
infa2dbt discover -i ./input/ \
    --schema-source snowflake \
    --database MY_DB --schema RAW_SOURCES

# 3. Generate EWI assessment report
infa2dbt report -p ./output/dbt_project -f both

# 4. Validate the generated project (compile + run + test)
infa2dbt validate -p ./output/dbt_project --run-tests

# 5. Deploy to Snowflake
infa2dbt deploy -p ./output/dbt_project \
    -d MY_DB -s MY_SCHEMA \
    --mode direct --connection myconnection

# 6. Execute on Snowflake (run models + tests)
snow dbt execute -c myconnection \
    --database MY_DB --schema MY_SCHEMA MY_PROJECT run
snow dbt execute -c myconnection \
    --database MY_DB --schema MY_SCHEMA MY_PROJECT test

# 7. Push to Git
infa2dbt git-push -p ./output/dbt_project \
    --remote-url https://github.com/org/repo.git \
    -b main -m "Initial migration"

# 8. (Optional) Schedule recurring execution
infa2dbt deploy -p ./output/dbt_project \
    --mode schedule --cron "0 2 * * *"
```
