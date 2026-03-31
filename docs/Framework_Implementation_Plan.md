# Framework Implementation Plan
## Generic LLM-Powered ETL-to-dbt Migration Framework for Snowflake

| Field | Value |
|-------|-------|
| **Document Version** | 1.0 |
| **Project** | Informatica-to-dbt Generic Migration Framework |
| **Approach** | LLM-Powered (Snowflake Cortex) — SnowConvert AI-aligned design |
| **Target Platform** | Snowflake-Native dbt Projects |
| **Current Status** | POC Complete (9 mappings, 51 models, 170 tests, 51/51 PASS) |
| **Next Phase** | Generic Framework Build |

---

## Table of Contents

1. [Framework Vision](#1-framework-vision)
2. [SnowConvert AI vs Our Framework — Comparison](#2-snowconvert-ai-vs-our-framework--comparison)
3. [Architecture Design](#3-architecture-design)
4. [dbt Project Structure Decision](#4-dbt-project-structure-decision)
5. [Implementation Steps (8 Components)](#5-implementation-steps-8-components)
6. [File Inventory](#6-file-inventory)
7. [Verification Plan](#7-verification-plan)
8. [Implementation Procedure](#8-implementation-procedure)

---

## 1. Framework Vision

### Goal

Build a **generic, reusable framework** — similar in workflow to Snowflake's SnowConvert AI — that automatically converts **any** Informatica PowerCenter XML workflow into a Snowflake-native dbt project. The framework:

- Takes Informatica XML files as input (exported from PowerCenter)
- Uses **LLM (Snowflake Cortex)** instead of rule-based conversion for maximum flexibility
- Generates a complete dbt project (staging → intermediate → marts) with automated tests
- Validates the output locally via `dbt compile` and `dbt run`
- Pushes the project to Git for version control
- Enables Snowflake to pull from Git and run the project natively
- Produces identical output on re-runs (via caching)
- Works for **any** Informatica ETL workload, not just the 9 sample mappings

### What Exists Today (POC)

| Component | Status | Details |
|-----------|--------|---------|
| XML Parser | Complete | Parses all Informatica PowerCenter XML elements (sources, targets, 30+ transform types, connectors, shortcuts) |
| Complexity Analyzer | Complete | Scores mappings 0-100 across 11 dimensions, picks strategy (DIRECT/STAGED/LAYERED/COMPLEX) |
| Transformation Registry | Complete | 30+ Informatica transform types mapped to dbt patterns, 60+ function conversions |
| LLM Code Generator | Complete | Cortex-powered with chunking for large mappings, few-shot prompts, strategy-specific generation |
| Self-Healing Loop | Complete | Validates output, sends errors back to LLM for correction (up to 2 attempts) |
| Quality Scorer | Complete | Scores generated code 0-100 across 5 dimensions |
| Validators | Complete | 9 SQL checks + YAML structure + project-level ref/DAG validation |
| Post-Processor | Complete | Cleans Informatica residuals (IIF→IFF, ISNULL, etc.), 15+ pattern replacements |
| Persistence | Partial | Writes to local filesystem or Snowflake stage |
| **Project Merger** | **Missing** | Each mapping produces an isolated mini-project; no mechanism to merge |
| **CLI** | **Missing** | Only `notebook_entry.py` exists; no proper command-line tool |
| **Git Integration** | **Missing** | Zero Git code; project is not even a Git repository |
| **Snowflake Git Deploy** | **Missing** | No `CREATE GIT REPOSITORY` or Git-based deployment code |
| **Output Caching** | **Missing** | LLM calls are non-deterministic; re-runs produce different output |
| **Source Auto-Discovery** | **Missing** | `source_map.json` and `table_columns.json` manually created |
| **Live dbt Validation** | **Missing** | No `dbt compile` / `dbt run` integration |
| **Assessment Reports** | **Missing** | No EWI-style reports |

---

## 2. SnowConvert AI vs Our Framework — Comparison

### 2.1 Architecture Comparison

#### SnowConvert AI (Rule-Based)

```
ETL Package Files ──► SnowConvert Engine ──► dbt Projects + Orchestration SQL
 (SSIS .dtsx or       (Deterministic        (One dbt project per Data Flow Task
  Informatica .xml)    rule-based engine)     + Snowflake TASKs for control flow)
```

- **Conversion engine**: Deterministic, rule-based. Hardcoded translation rules for each SSIS/Informatica component type.
- **Speed**: Fast (milliseconds per mapping) — no external API calls.
- **Output**: Per-Data-Flow-Task isolated dbt projects + Snowflake TASK chains for orchestration.
- **EWIs**: Generates Errors/Warnings/Informational messages for unsupported patterns.
- **Informatica support**: Added in v2.15.0 (March 2026) — still early. Supports: TRUNC, DECODE, ADD_TO_DATE, LTRIM, RTRIM, MAX, SUM, MIN, GET_DATE_PART, IS_NUMBER, IS_DATE, TO_DATE, Sorter, Sequence Generator, Source Qualifier overrides.

#### Our Framework (LLM-Powered)

```
Informatica XML ──► Parser + Analyzer ──► Cortex LLM ──► Post-Process + Validate ──► Merged dbt Project
                    (Local Python)         (Snowflake)     (Local Python)              (Local filesystem)
```

- **Conversion engine**: LLM-powered (Snowflake Cortex). Uses comprehensive 285-line system prompt with 57 conversion rules, few-shot examples, and strategy-specific instructions.
- **Speed**: Slower (30-60 seconds per mapping) — requires LLM API calls.
- **Output**: Single consolidated dbt project with all mappings merged. Models nested by mapping directory.
- **Self-healing**: Validates output, sends errors back to LLM for correction (up to 2 attempts).
- **Testing**: Auto-generates 170+ data tests (not_null, unique, accepted_values, relationships, accepted_range).

### 2.2 Feature Parity Matrix

| Feature | SnowConvert AI | Our Framework (Current) | Our Framework (After Plan) |
|---------|---------------|------------------------|---------------------------|
| **Input formats** | SSIS `.dtsx` + Informatica `.xml` | Informatica `.xml` | Informatica `.xml` (extensible to SSIS) |
| **Conversion engine** | Rule-based (deterministic) | LLM-powered (Cortex) | LLM-powered + cache for determinism |
| **Output: dbt structure** | staging/intermediate/marts | staging/intermediate/marts | staging/intermediate/marts (identical) |
| **Output: orchestration** | Snowflake TASKs + procedures | Not yet | Snowflake TASKs |
| **Output: EWI reports** | HTML reports | Not yet | HTML assessment reports |
| **Auto-generated tests** | None | 170+ tests | 170+ tests |
| **Self-healing** | None (manual EWI fixes) | LLM correction loop (2 attempts) | LLM correction loop (2 attempts) |
| **Complexity analysis** | Basic | 11-dimension scoring (0-100) | 11-dimension scoring (0-100) |
| **Function conversion** | ~15 functions (early) | 60+ functions | 60+ functions |
| **Transform types** | Limited (Sorter, Seq Gen, SQ) | 30+ types (full registry) | 30+ types (full registry) |
| **Source discovery** | Requires user DDL scripts | Manual `source_map.json` | Auto from Snowflake or XML |
| **Project merge** | Separate projects per task | Manual (done by hand) | Automated merger |
| **CLI tool** | `scai` command-line | Only `notebook_entry.py` | Full `infa2dbt` CLI |
| **Git integration** | Manual / CI-CD docs | None | Built-in git-push + PR |
| **Snowflake deploy** | `snow dbt deploy` | `snow dbt deploy` (manual) | `snow dbt deploy` + Git repo deploy |
| **CI/CD** | GitHub Actions (documented) | None | GitHub Actions (built-in) |
| **Caching/reproducibility** | Rule-based = deterministic | Non-deterministic | SHA-256 cache layer |
| **Quality scoring** | None | 5-dimension quality score | 5-dimension quality score |
| **Naming sanitization** | Lowercase + underscore + prefix | `_sanitize_project_name()` | Enhanced (SnowConvert-aligned rules) |

### 2.3 Scoring Comparison

| Dimension | SnowConvert AI | Our Framework (Current) | Our Framework (After Plan) |
|-----------|:--------------:|:-----------------------:|:--------------------------:|
| **Flexibility** (handles diverse ETL patterns) | 6/10 | 9/10 | 9/10 |
| **Determinism** (same input → same output) | 10/10 | 3/10 | 8/10 |
| **Speed** (time per mapping) | 9/10 | 5/10 | 5/10 |
| **Output quality** (generated code correctness) | 7/10 | 8/10 | 8/10 |
| **dbt structure** (best practices compliance) | 9/10 | 9/10 | 9/10 |
| **Testing** (auto-generated data tests) | 3/10 | 9/10 | 9/10 |
| **Orchestration** (TASK/schedule generation) | 9/10 | 0/10 | 7/10 |
| **CI/CD** (Git + deploy automation) | 8/10 | 0/10 | 8/10 |
| **Self-healing** (auto-fix errors) | 0/10 | 8/10 | 8/10 |
| **ETL breadth** (SSIS + Informatica + ...) | 8/10 | 6/10 | 6/10 |
| **Overall** | **~77/100** | **~57/100** | **~85/100** |

### 2.4 Key Advantages of Our Approach

1. **LLM handles complexity that rules cannot**: Informatica mappings with deeply nested expressions, multiple routers, conditional lookups, and complex SCD patterns. SnowConvert generates EWIs for these; our framework generates working code.

2. **Auto-generated tests**: 170+ tests out of the box. SnowConvert generates zero tests — the user must write them manually.

3. **Self-healing**: When the LLM makes a mistake, the framework catches it (via 9 SQL checks, YAML validation, project-level ref checks), sends the errors back to the LLM, and gets a corrected output. SnowConvert has no equivalent.

4. **Quality scoring**: Every generated mapping gets a 0-100 quality score across 5 dimensions, providing transparency into conversion quality.

### 2.5 Where SnowConvert AI is Better

1. **Speed**: Rule-based conversion is instant. Our LLM calls take 30-60 seconds per mapping.
2. **Determinism**: Rules always produce the same output. Our framework needs a cache layer.
3. **ETL breadth**: SnowConvert supports both SSIS and Informatica. We currently only support Informatica.
4. **Orchestration**: SnowConvert generates complete TASK chains for SSIS control flow. We don't yet generate orchestration SQL.

---

## 3. Architecture Design

### 3.1 Local Execution Flow

```
                        LOCAL MACHINE                              SNOWFLAKE
                   ┌──────────────────────────┐              ┌──────────────────┐
                   │                          │              │                  │
 XML files ───────►│  1. Parse XML            │              │                  │
                   │  2. Check cache           │              │                  │
                   │  3. Analyze complexity    │              │                  │
                   │  4. Chunk for LLM         │──LLM call──►│  Cortex LLM      │
                   │  5. Parse LLM response    │◄──response──│  (claude-4-sonnet)│
                   │  6. Post-process          │              │                  │
                   │  7. Validate (SQL/YAML)   │              │                  │
                   │  8. Self-heal if needed   │──LLM call──►│  Cortex LLM      │
                   │  9. Score quality         │◄──response──│                  │
                   │ 10. Cache output          │              │                  │
                   │ 11. Merge into project    │              │                  │
                   │ 12. dbt compile           │──SQL────────►│  Compile check   │
                   │ 13. Write locally         │              │                  │
                   └──────────┬────────────────┘              └──────────────────┘
                              │
                   ┌──────────▼────────────────┐
                   │  14. Git commit + push     │
                   └──────────┬────────────────┘
                              │
                   ┌──────────▼────────────────┐              ┌──────────────────┐
                   │  15. Deploy to Snowflake   │─────────────►│  CREATE DBT      │
                   │  (snow dbt deploy or       │              │  PROJECT FROM    │
                   │   from Git repository)     │              │  @git_repo/main  │
                   └───────────────────────────┘              └──────────────────┘
```

### 3.2 Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLI (cli.py)                                │
│  infa2dbt convert | discover | validate | deploy | git-push        │
└────────┬──────────┬──────────┬──────────┬──────────┬───────────────┘
         │          │          │          │          │
    ┌────▼────┐ ┌──▼───┐ ┌───▼────┐ ┌───▼───┐ ┌───▼────┐
    │ XML     │ │Schema│ │ dbt    │ │Deploy │ │ Git    │
    │ Parser  │ │Disco-│ │Validate│ │(direct│ │Manager │
    │         │ │very  │ │(compile│ │ + git)│ │(commit,│
    │         │ │      │ │ + run) │ │       │ │ push)  │
    └────┬────┘ └──────┘ └───────┘ └───────┘ └────────┘
         │
    ┌────▼────────────────────────────────────────────┐
    │              Orchestrator                        │
    │  analyze → cache check → chunk → LLM generate   │
    │  → merge chunks → post-process → validate       │
    │  → self-heal → score → cache store              │
    └────────┬────────────────────────────────────────┘
             │
    ┌────────▼────────────────────────────────────────┐
    │            Project Merger                        │
    │  Consolidate mapping outputs into one project:  │
    │  - Merge dbt_project.yml vars                   │
    │  - Consolidate _sources.yml                     │
    │  - Union packages.yml                           │
    │  - Copy macros (dedup by hash)                  │
    │  - Write models/<mapping>/staging|int|marts/    │
    └─────────────────────────────────────────────────┘
```

### 3.3 What Runs Where

| Operation | Where it runs | Requires Snowflake? |
|-----------|--------------|---------------------|
| XML parsing | Local Python | No |
| Complexity analysis | Local Python | No |
| Chunking | Local Python | No |
| LLM code generation | Snowflake Cortex (via connection) | **Yes** |
| Post-processing | Local Python | No |
| Validation (static) | Local Python | No |
| Self-healing (LLM) | Snowflake Cortex | **Yes** |
| Quality scoring | Local Python | No |
| Output caching | Local filesystem | No |
| Project merging | Local Python | No |
| `dbt compile` validation | Via Snowflake (SQL) | **Yes** |
| Git operations | Local git CLI | No |
| Snowflake deployment | Snowflake (snow CLI or SQL) | **Yes** |

---

## 4. dbt Project Structure Decision

### 4.1 SnowConvert AI Structure

SnowConvert creates **separate dbt projects per Data Flow Task**:

```
Output/ETL/
├── etl_configuration/                    # Shared infra (SSIS-specific)
│   ├── tables/control_variables_table.sql
│   ├── functions/GetControlVariableUDF.sql
│   └── procedures/UpdateControlVariable.sql
├── {PackageName}/
│   ├── {PackageName}.sql                 # Orchestration TASK
│   └── {DataFlowTaskName}/              # Isolated dbt project
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│       │   ├── sources.yml
│       │   ├── staging/
│       │   │   └── stg_raw__{component_name}.sql
│       │   ├── intermediate/
│       │   │   └── int_{component_name}.sql
│       │   └── marts/
│       │       └── {destination_component_name}.sql
│       ├── macros/
│       ├── seeds/
│       └── tests/
└── {AnotherPackage}/
    └── ...                               # Another isolated dbt project
```

**Why SnowConvert uses this**: SSIS Data Flow Tasks are inherently isolated — each operates independently. Separate projects make sense.

### 4.2 Our Framework Structure (Recommended — Keep As-Is)

Our framework creates **one consolidated dbt project** with mappings nested by directory:

```
dbt_project/
├── dbt_project.yml              # Single project config with all vars (32 lookups)
├── profiles.yml                 # Snowflake connection
├── packages.yml                 # dbt_utils v1.3.0
├── macros/                      # Shared macros
│   ├── generate_md5_hash.sql
│   └── hash_md5_50_fields.sql
├── models/
│   ├── m_BL_FF_ZJ_JOURNALS_STG/    # Mapping 1
│   │   ├── staging/
│   │   │   ├── _sources.yml
│   │   │   ├── _stg__schema.yml
│   │   │   └── stg_f0911_jnl_stg.sql, stg_f0901_jnl_stg.sql, ...
│   │   ├── intermediate/
│   │   │   ├── _int__schema.yml
│   │   │   └── int_journals_base.sql, int_journals_reverse.sql, ...
│   │   └── marts/
│   │       ├── _marts__schema.yml
│   │       └── fct_zj_journals_staging.sql, fct_zj_journals_flat_file.sql
│   ├── m_INCR_DM_DIM_EQUIPMENT/     # Mapping 2
│   │   ├── staging/    (8 models)
│   │   ├── intermediate/ (5 models)
│   │   └── marts/       (1 model)
│   ├── m_DI_ITEM_MTRL_MASTER/       # Mapping 3
│   └── ...                           # N more mappings
├── seeds/
├── snapshots/
└── tests/
```

### 4.3 Why Our Structure is Better for Informatica

| Aspect | SnowConvert (Separate Projects) | Ours (Consolidated) | Winner |
|--------|--------------------------------|---------------------|--------|
| Cross-mapping `ref()` | Not possible (separate projects) | Full `ref()` across mappings | **Ours** |
| Shared sources | Each project duplicates source definitions | Single consolidated `_sources.yml` | **Ours** |
| Shared macros | Duplicated per project | One `macros/` directory | **Ours** |
| Deployment | Deploy N separate dbt projects | Deploy 1 project | **Ours** |
| Naming collision | No risk (isolated) | Prevented by mapping-name prefix | Tie |
| Selective execution | Separate project = natural isolation | `dbt run --select tag:<mapping>` | Tie |
| Traceability | Mapping → project (clear) | Mapping → directory (equally clear) | Tie |

**Decision**: Keep our current structure. Add dbt `tags` per mapping for selective execution.

### 4.4 Minor Enhancement: Add Tags

Add to each model's config block:

```sql
{{ config(materialized='view', tags=['m_BL_FF_ZJ_JOURNALS_STG']) }}
```

Enables:
```sql
-- Run only one mapping's models
EXECUTE DBT PROJECT ... ARGS = 'run --select tag:m_BL_FF_ZJ_JOURNALS_STG';
```

---

## 5. Implementation Steps (8 Components)

### Step 1: CLI Entry Point

**What**: Create a `click`-based CLI that mirrors SnowConvert AI's project workflow.

**Commands**:

```bash
# Full conversion (like SnowConvert's "Convert code and ETL projects")
infa2dbt convert \
  --input ./informatica_xmls/ \
  --output ./my_dbt_project/ \
  --connection myconnection \
  --mode new              # or "merge" to add to existing project

# Single XML conversion (add new mapping to existing project)
infa2dbt convert \
  --input ./new_workflow.XML \
  --output ./my_dbt_project/ \
  --mode merge

# Discover source tables from Snowflake
infa2dbt discover \
  --database TPC_DI_RAW_DATA \
  --schema MOCK_SOURCES \
  --connection myconnection

# Validate the generated project locally
infa2dbt validate --project ./my_dbt_project/

# Deploy to Snowflake
infa2dbt deploy \
  --project ./my_dbt_project/ \
  --database DB --schema SCHEMA \
  --connection myconnection

# Push to Git
infa2dbt git-push \
  --project ./my_dbt_project/ \
  --remote origin --branch main

# Show conversion history
infa2dbt status --project ./my_dbt_project/
```

**New file**: `informatica_to_dbt/cli.py`
**Modify**: `pyproject.toml` — add `click` dependency, `[project.scripts] infa2dbt = "informatica_to_dbt.cli:main"`

---

### Step 2: Project Merger

**What**: The critical missing piece. Assembles individual mapping outputs into one consolidated dbt project.

**New files**:
- `informatica_to_dbt/merger/project_merger.py` — Core merge logic
- `informatica_to_dbt/merger/source_consolidator.py` — Deduplicate `_sources.yml`
- `informatica_to_dbt/merger/conflict_resolver.py` — Handle naming conflicts

**Logic**:

1. **`--mode new`**: Scaffold fresh project directory:
   - Generate `dbt_project.yml` with model configs for all layers
   - Generate `profiles.yml` with Snowflake connection template
   - Generate `packages.yml` with detected dependencies
   - Create directory structure: `models/`, `macros/`, `seeds/`, `snapshots/`, `tests/`

2. **`--mode merge`**: Read existing project, add new mapping:
   - Models → `models/<mapping_name>/staging|intermediate|marts/`
   - `dbt_project.yml` vars → append new lookup variables (skip duplicates)
   - `packages.yml` → union dependencies (keep highest version)
   - Macros → copy to `macros/` if not already present (compare by content hash)
   - Sources → consolidate all `_sources.yml` files (globally unique names)

3. **Merge metadata**: Write `.infa2dbt/manifest.json`:
   ```json
   {
     "version": "1.0.0",
     "mappings": {
       "m_BL_FF_ZJ_JOURNALS_STG": {
         "xml_hash": "sha256:abc...",
         "converted_at": "2026-03-31T10:00:00Z",
         "files_generated": 10,
         "quality_score": 85
       }
     }
   }
   ```

---

### Step 3: Output Caching

**What**: Ensures same XML input produces same dbt output (solves LLM non-determinism).

**New file**: `informatica_to_dbt/cache/conversion_cache.py`

**Logic**:
- **Cache key** = SHA-256 of: `XML content + converter_version + prompt_version + LLM model name`
- **Before LLM call**: Check `.infa2dbt/cache/<hash>/` directory
  - If exists and valid → return cached `GeneratedFile` list (skip LLM entirely)
  - If not → proceed with LLM generation
- **After successful conversion**: Persist `GeneratedFile` list to cache directory
- **CLI flags**:
  - `--no-cache` → force regeneration (ignore cache)
  - `--clear-cache` → wipe all cached outputs

**Cache structure**:
```
.infa2dbt/cache/
├── a1b2c3d4.../         # SHA-256 prefix
│   ├── metadata.json    # XML filename, timestamp, quality score
│   └── files/           # Cached GeneratedFile objects
│       ├── models/staging/stg_f0911.sql
│       ├── models/staging/_sources.yml
│       └── ...
```

---

### Step 4: Source Discovery

**What**: Eliminate the manual `source_map.json` / `table_columns.json` dependency.

**New file**: `informatica_to_dbt/discovery/schema_discovery.py`

**Three modes**:

1. **Snowflake mode** (`infa2dbt discover --database DB --schema SCHEMA`):
   - Query `INFORMATION_SCHEMA.COLUMNS` → build table→column map
   - Output: `source_map.json` auto-generated

2. **XML mode** (automatic during conversion):
   - Extract source/target tables and columns from Informatica XML
   - The parser already captures `Source.fields` and `Target.fields`
   - No Snowflake connection needed

3. **Manual mode** (fallback):
   - User provides `source_map.json` via `--source-map` flag
   - Backwards compatible with current workflow

---

### Step 5: Live dbt Validation

**What**: Integrate `dbt compile` and `dbt run` into the pipeline for real validation.

**New file**: `informatica_to_dbt/validation/dbt_validator.py`

**Logic**:
- After merger writes the project, run `dbt compile` via subprocess
- Parse stdout/stderr for errors → classify as EWIs
- Optionally run `dbt run` for full execution validation
- Generate **assessment report** (HTML) with:
  - Per-mapping: complexity score, strategy, files generated, EWIs
  - Aggregate: total models, tests, pass rate, quality scores
  - Similar to SnowConvert's ETL Replatform Component Summary Report

---

### Step 6: Git Integration

**What**: Built-in Git support for version control workflow.

**New file**: `informatica_to_dbt/git/git_manager.py`

**Operations**:
- `init_repo(path)` — `git init` if not already a repo
- `create_branch(name)` — create feature branch from current HEAD
- `commit(message)` — stage dbt project files, commit with descriptive message
- `push(remote, branch)` — push to GitHub/GitLab/Azure DevOps
- `create_pr(title, body)` — create pull request via `gh` CLI (if available)

**CLI integration**: One command does convert + merge + git push:
```bash
infa2dbt convert \
  --input ./new_workflows/ \
  --output ./my_dbt_project/ \
  --mode merge \
  --git-push \
  --branch feature/new-journals-mapping
```

---

### Step 7: Snowflake Deployment

**What**: Deploy the dbt project to Snowflake (direct or via Git).

**New file**: `informatica_to_dbt/deployment/deployer.py`

**Three deployment modes**:

1. **Direct mode** (existing — uses `snow dbt deploy`):
   ```bash
   infa2dbt deploy --project ./my_dbt_project/ --database DB --schema SCHEMA
   # Runs: snow dbt deploy --source ./my_dbt_project/ --database DB --schema SCHEMA PROJECT_NAME
   ```

2. **Git mode** (new — Snowflake pulls from Git):
   ```bash
   infa2dbt deploy --from-git --git-repo my_git_repo --branch main
   ```
   Generates and executes:
   ```sql
   -- One-time setup
   CREATE OR REPLACE API INTEGRATION git_api_integration ...;
   CREATE OR REPLACE SECRET git_secret ...;
   CREATE OR REPLACE GIT REPOSITORY my_git_repo ...;

   -- Deploy from Git
   ALTER GIT REPOSITORY my_git_repo FETCH;
   CREATE OR REPLACE DBT PROJECT my_project
     FROM '@my_git_repo/branches/main/dbt_project';
   ```

3. **Schedule mode** (new — TASK-based orchestration):
   ```bash
   infa2dbt deploy ... --schedule "0 2 * * *"
   ```
   Generates:
   ```sql
   CREATE OR REPLACE TASK dbt_nightly_run
     WAREHOUSE = SMALL_WH
     SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
   AS
     EXECUTE DBT PROJECT DB.SCHEMA.PROJECT_NAME ARGS = 'run';
   ```

---

### Step 8: Generic Documentation & Reports

**What**: Update all documentation to be framework-level (not 9-XML specific).

**Changes**:
- Rewrite `docs/Technical_Design_Document.md` → generic framework doc with CLI reference
- Add `docs/diagrams/06_framework_pipeline.html` → full pipeline diagram including Git/deploy
- Update `docs/diagrams/01_high_level_architecture.html` → add Git integration layer
- Add `docs/CLI_Reference.md` → full CLI command reference
- Generate **per-run HTML assessment reports** (SnowConvert-style)

---

## 6. File Inventory

### 6.1 New Files to Create

| File | Purpose |
|------|---------|
| `informatica_to_dbt/cli.py` | Click-based CLI (primary user interface) |
| `informatica_to_dbt/merger/__init__.py` | Merger module init |
| `informatica_to_dbt/merger/project_merger.py` | Core merge logic (new project + merge into existing) |
| `informatica_to_dbt/merger/source_consolidator.py` | Deduplicate `_sources.yml` across mappings |
| `informatica_to_dbt/merger/conflict_resolver.py` | Handle naming conflicts |
| `informatica_to_dbt/cache/__init__.py` | Cache module init |
| `informatica_to_dbt/cache/conversion_cache.py` | SHA-256 fingerprint + cache lookup/store |
| `informatica_to_dbt/discovery/__init__.py` | Discovery module init |
| `informatica_to_dbt/discovery/schema_discovery.py` | Auto-discover source schema from Snowflake or XML |
| `informatica_to_dbt/git/__init__.py` | Git module init |
| `informatica_to_dbt/git/git_manager.py` | Git init/branch/commit/push/PR operations |
| `informatica_to_dbt/deployment/__init__.py` | Deployment module init |
| `informatica_to_dbt/deployment/deployer.py` | Snowflake deploy (direct + Git + TASK scheduling) |
| `informatica_to_dbt/validation/__init__.py` | Validation module init |
| `informatica_to_dbt/validation/dbt_validator.py` | Live `dbt compile`/`dbt run` validation |
| `informatica_to_dbt/reports/__init__.py` | Reports module init |
| `informatica_to_dbt/reports/ewi_report.py` | HTML assessment report generator |
| `tests/unit/test_merger.py` | Merger unit tests |
| `tests/unit/test_cache.py` | Cache unit tests |
| `tests/unit/test_discovery.py` | Discovery unit tests |
| `tests/unit/test_git_manager.py` | Git integration tests |
| `tests/integration/test_end_to_end_framework.py` | Full pipeline integration test |
| `.gitignore` | Git ignore rules |
| `docs/Framework_Implementation_Plan.md` | This document |
| `docs/CLI_Reference.md` | CLI command reference |
| `docs/diagrams/06_framework_pipeline.html` | Full pipeline diagram |

### 6.2 Existing Files to Modify

| File | Changes |
|------|---------|
| `pyproject.toml` | Add `click` dependency, `[project.scripts]` entry point |
| `informatica_to_dbt/config.py` | Add fields: `project_dir`, `git_remote`, `git_branch`, `cache_dir`, `cache_enabled`, `converter_version`, `connection_name` |
| `informatica_to_dbt/orchestrator.py` | Add post-conversion hooks for merger + cache integration |
| `informatica_to_dbt/generator/prompt_builder.py` | Add `PROMPT_VERSION` constant for cache keying |
| `informatica_to_dbt/__init__.py` | Export new modules |
| `docs/Technical_Design_Document.md` | Rewrite as generic framework documentation |
| `docs/diagrams/01_high_level_architecture.html` | Add Git/CI-CD layer |

---

## 7. Verification Plan

### 7.1 Per-Step Verification

| Step | Verification | Command |
|------|-------------|---------|
| 1. CLI | Smoke test all commands | `infa2dbt --help`, `infa2dbt convert --help` |
| 2. Merger | Convert 2 XMLs → merge → valid project | `infa2dbt convert --input ./input/wf_AM_DI_CUSTOMER.XML --output /tmp/test --mode new` then `infa2dbt convert --input ./input/wf_AP_FF_CITIBANK_VCA.XML --output /tmp/test --mode merge` |
| 3. Cache | Convert same XML twice → second returns instantly | Time both runs; second should skip LLM calls |
| 4. Discovery | Auto-discover from Snowflake | `infa2dbt discover --database TPC_DI_RAW_DATA --schema MOCK_SOURCES` |
| 5. Validation | `dbt compile` succeeds on merged project | `infa2dbt validate --project /tmp/test` |
| 6. Git | Commit + push works | `infa2dbt git-push --project /tmp/test --branch test-branch` |
| 7. Deploy | Native project works | `infa2dbt deploy --project /tmp/test --database TPC_DI_RAW_DATA --schema DBT_INGEST` |
| 8. Docs | All documentation is generic | Manual review |

### 7.2 End-to-End Verification

```bash
# 1. Convert all 9 XMLs into a new project
infa2dbt convert --input ./input/ --output /tmp/full_test --mode new --connection myconnection

# 2. Validate locally
infa2dbt validate --project /tmp/full_test

# 3. Re-run (should use cache — no LLM calls)
infa2dbt convert --input ./input/ --output /tmp/full_test --mode new --connection myconnection

# 4. Git push
infa2dbt git-push --project /tmp/full_test --branch feature/full-migration

# 5. Deploy to Snowflake
infa2dbt deploy --project /tmp/full_test --database TPC_DI_RAW_DATA --schema DBT_INGEST --connection myconnection

# 6. Execute in Snowflake
EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run';
-- Expected: 51/51 PASS

# 7. Run tests
EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'test';
-- Expected: 170 tests pass
```

### 7.3 Unit Test Verification

```bash
# All existing tests must continue to pass
pytest tests/ -v

# New tests
pytest tests/unit/test_merger.py -v
pytest tests/unit/test_cache.py -v
pytest tests/unit/test_discovery.py -v
pytest tests/unit/test_git_manager.py -v
pytest tests/integration/test_end_to_end_framework.py -v
```

---

## 8. Implementation Procedure

### Phase 1: Foundation (Steps 1-2)

**Priority**: Highest — these are prerequisites for everything else.

| Order | Task | Dependencies |
|-------|------|-------------|
| 1.1 | Initialize Git repo (`.gitignore`, `git init`) | None |
| 1.2 | Add `click` to `pyproject.toml` | None |
| 1.3 | Create `cli.py` with `convert` command skeleton | 1.2 |
| 1.4 | Create `merger/project_merger.py` (new mode) | None |
| 1.5 | Create `merger/source_consolidator.py` | None |
| 1.6 | Create `merger/conflict_resolver.py` | None |
| 1.7 | Integrate merger into `orchestrator.py` | 1.4, 1.5, 1.6 |
| 1.8 | Wire CLI `convert` command to orchestrator + merger | 1.3, 1.7 |
| 1.9 | Write `tests/unit/test_merger.py` | 1.4 |
| 1.10 | Test: convert 2 XMLs → merge → valid project | 1.8 |

### Phase 2: Reliability (Steps 3-4)

**Priority**: High — enables reproducible, self-service usage.

| Order | Task | Dependencies |
|-------|------|-------------|
| 2.1 | Create `cache/conversion_cache.py` | None |
| 2.2 | Add `PROMPT_VERSION` to `prompt_builder.py` | None |
| 2.3 | Integrate cache into orchestrator (pre-LLM check + post-LLM store) | 2.1, 2.2 |
| 2.4 | Create `discovery/schema_discovery.py` | None |
| 2.5 | Wire `infa2dbt discover` CLI command | 2.4 |
| 2.6 | Write `tests/unit/test_cache.py` and `test_discovery.py` | 2.1, 2.4 |
| 2.7 | Test: same XML twice → second cached, identical output | 2.3 |

### Phase 3: Validation & Deployment (Steps 5-7)

**Priority**: Medium-High — completes the production pipeline.

| Order | Task | Dependencies |
|-------|------|-------------|
| 3.1 | Create `validation/dbt_validator.py` (`dbt compile` subprocess) | Phase 1 |
| 3.2 | Wire `infa2dbt validate` CLI command | 3.1 |
| 3.3 | Create `git/git_manager.py` | None |
| 3.4 | Wire `infa2dbt git-push` CLI command | 3.3 |
| 3.5 | Create `deployment/deployer.py` (direct + Git + TASK) | None |
| 3.6 | Wire `infa2dbt deploy` CLI command | 3.5 |
| 3.7 | Write `tests/unit/test_git_manager.py` | 3.3 |
| 3.8 | End-to-end test: XML → convert → merge → validate → git push → deploy | All above |

### Phase 4: Documentation & Reports (Step 8)

**Priority**: Medium — polish for customer delivery.

| Order | Task | Dependencies |
|-------|------|-------------|
| 4.1 | Create `reports/ewi_report.py` (HTML assessment report) | Phase 1 |
| 4.2 | Create `docs/diagrams/06_framework_pipeline.html` | None |
| 4.3 | Update `docs/Technical_Design_Document.md` → generic | None |
| 4.4 | Create `docs/CLI_Reference.md` | Phase 1 |
| 4.5 | Update `docs/diagrams/01_high_level_architecture.html` | None |

---

*This document serves as the complete implementation plan for the Generic Informatica-to-dbt Migration Framework.*
*Framework repository: `/Users/vicky/informatica-to-dbt/`*
*Current POC: 9 mappings, 51 models, 170 tests, 51/51 PASS on Snowflake*
