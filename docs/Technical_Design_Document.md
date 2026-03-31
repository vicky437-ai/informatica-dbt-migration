# Technical Design Document
## Informatica PowerCenter to dbt/Snowflake Migration Framework

| Field | Value |
|-------|-------|
| **Document Version** | 1.0 |
| **Project** | Informatica to dbt Migration Framework |
| **Target Platform** | Snowflake (Cloud-Native) |
| **Snowflake Account** | EPCKZEL-SQUADRON_US_EAST_1 |
| **Database** | TPC_DI_RAW_DATA |
| **Schema (Sources)** | MOCK_SOURCES |
| **Schema (Target)** | DBT_INGEST |
| **Status** | Deployed and Validated (51/51 PASS) |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Migration Scope](#3-migration-scope)
4. [Design Approach](#4-design-approach)
5. [dbt Project Structure](#5-dbt-project-structure)
6. [Data Flow Architecture](#6-data-flow-architecture)
7. [Transformation Patterns](#7-transformation-patterns)
8. [Snowflake-Native Deployment](#8-snowflake-native-deployment)
9. [Testing Strategy](#9-testing-strategy)
10. [Key Design Decisions](#10-key-design-decisions)
11. [Deployment Results](#11-deployment-results)
12. [Operational Runbook](#12-operational-runbook)
13. [Generic Framework Architecture](#13-generic-framework-architecture)
14. [30-Minute Demo Plan](#14-30-minute-demo-plan)

---

## 1. Executive Summary

This document describes the technical design and implementation of a migration framework that converts **Informatica PowerCenter ETL mappings** into **dbt (data build tool) models** running natively on **Snowflake**. The framework eliminates the dependency on Informatica's licensed, on-premise infrastructure and replaces it with a modern, cloud-native ELT architecture.

### Key Results

| Metric | Value |
|--------|-------|
| Informatica Mappings Migrated | 9 |
| dbt Models Generated | 51 |
| Automated Data Tests | 170 |
| Informatica Lookup Transforms Converted | 32 |
| Source Tables Integrated | 38 |
| Model Execution Pass Rate | 100% (51/51) |
| Deployment Mode | Snowflake-Native dbt Project |
| Execution Time | 69.29 seconds (all 51 models) |

### Architectural Diagrams

> **Important:** Open the following HTML files in a web browser to view the professional diagrams:

| Diagram | File | Description |
|---------|------|-------------|
| High-Level Architecture | [01_high_level_architecture.html](diagrams/01_high_level_architecture.html) | End-to-end system architecture from sources to Snowflake outputs |
| Data Flow Diagram | [02_data_flow_diagram.html](diagrams/02_data_flow_diagram.html) | Detailed model-level data flow across all 9 pipelines |
| Migration Process Flow | [03_migration_process_flow.html](diagrams/03_migration_process_flow.html) | 4-phase migration methodology with results |
| Snowflake Deployment Architecture | [04_snowflake_deployment_architecture.html](diagrams/04_snowflake_deployment_architecture.html) | Snowflake platform objects and execution methods |
| Before vs After Comparison | [05_before_after_comparison.html](diagrams/05_before_after_comparison.html) | Side-by-side legacy vs modern architecture with benefits |

---

## 2. Architecture Overview

### 2.1 Legacy Architecture (Informatica PowerCenter)

```
Oracle EBS (On-Premise) ──► Informatica Server (On-Premise) ──► Target Database
Flat Files (File Server) ──►    9 Mappings, 32 Lookups        ──► Tables
SAP/XML Sources         ──►    Licensed ETL Engine             ──►
```

**Limitations:**
- Proprietary, licensed ETL server requiring on-premise infrastructure
- GUI-based development with no version control (code-as-config)
- No built-in testing framework for data quality validation
- Limited data lineage visibility outside the Informatica ecosystem
- No native support for incremental/merge processing patterns
- Transformation executed on the ETL server (not in the data warehouse)

### 2.2 Modern Architecture (dbt + Snowflake)

```
Snowflake MOCK_SOURCES ──► dbt Staging (26 views) ──► dbt Intermediate (17 views) ──► dbt Marts (8 tables)
  (38 source tables)       Column rename, cast        Business logic, joins          Fact/Dim tables
                           Type conversion             Lookup resolution              Incremental merge
```

**Advantages:**
- **Zero infrastructure**: dbt runs natively inside Snowflake (no ETL server)
- **SQL-first**: All transformations are pure SQL, version-controlled in Git
- **Built-in testing**: 170 automated tests run with every deployment
- **Full lineage**: DAG-based dependency tracking visible in Snowsight
- **Incremental processing**: Native MERGE support for efficient data loads
- **ELT pattern**: Transformations execute on Snowflake compute (push-down)

---

## 3. Migration Scope

### 3.1 Informatica Mappings Converted

| # | Informatica Mapping | Domain | dbt Models | Materialization |
|---|---------------------|--------|------------|-----------------|
| 1 | m_BL_FF_ZJ_JOURNALS_STG | Journal Entries (Staging) | 10 (5 stg + 3 int + 2 marts) | Views + Incremental + Table |
| 2 | m_BL_FF_ZJ_JOURNALS | Journal Entries (Final) | 10 (5 stg + 3 int + 4 marts) | Views + Incremental + Table |
| 3 | m_INCR_DM_DIM_EQUIPMENT | Equipment Dimension | 14 (8 stg + 5 int + 1 marts) | Views + Incremental |
| 4 | m_DI_ITEM_MTRL_MASTER | Material Master Dimension | 8 (2 stg + 5 int + 1 marts) | Views + Incremental |
| 5 | m_AM_DI_CUSTOMER | Customer Dimension | 1 (stg only) | View |
| 6 | m_AP_FF_CITIBANK_VCA | Citibank Virtual Card | 1 (stg only) | View |
| 7 | m_CM_Z1_DAILY_SALES_FEED | Daily Sales Feed | 1 (stg only) | View |
| 8 | m_FF_AT_Z1_GL_ACCOUNT | GL Account (Z1) | 1 (stg only) | View |
| 9 | m_FF_AT_ZJ_GL_ACCOUNT | GL Account (ZJ) | 1 (stg only) | View |

### 3.2 Source Systems

| Source Type | Source Name | Tables | Schema |
|-------------|------------|--------|--------|
| Oracle EBS (JD Edwards) | `oracle_raw` | f0911, f0901, f0011, f0092, f01151, s_zj_journals, t_ap_citibank_vca, t_cm_z1_sales | MOCK_SOURCES |
| Flat Files (CSV) | `flat_file_raw` | ff_bl_zj_journals_src | MOCK_SOURCES |
| **Total** | **2 sources** | **38 tables** | |

### 3.3 Informatica Components Converted

| Informatica Component | dbt Equivalent | Count |
|-----------------------|----------------|-------|
| Source Qualifier | `source()` references in staging models | 38 |
| Expression Transform | SQL expressions in staging/intermediate models | ~100+ |
| Lookup Transform | `dbt var()` with subquery lookups | 32 |
| Filter Transform | `WHERE` clauses in intermediate models | ~15 |
| Joiner Transform | SQL `JOIN` in intermediate models | ~20 |
| Router Transform | `CASE WHEN` + `UNION ALL` in intermediate/marts | ~8 |
| Update Strategy | `is_incremental()` + `MERGE` in marts models | 4 |
| Aggregator Transform | `GROUP BY` in intermediate models | ~5 |
| Sequence Generator | `ROW_NUMBER()` / Snowflake sequences | ~3 |

---

## 4. Design Approach

### 4.1 Conversion Methodology

The migration follows a **4-phase methodology**:

> See diagram: [03_migration_process_flow.html](diagrams/03_migration_process_flow.html)

**Phase 1: Assessment & Extraction**
- Export Informatica PowerCenter mapping XML files
- Analyze source-to-target mappings, transformation logic, and lookup dependencies
- Create `source_map.json` mapping Informatica sources to Snowflake tables
- Provision mock source tables in Snowflake `MOCK_SOURCES` schema

**Phase 2: Automated Conversion (Python + AI-Assisted)**
- Parse Informatica XML to extract transformation logic programmatically
- Generate dbt SQL models following staging → intermediate → marts layering
- Generate YAML schema files with 170 data quality tests
- Convert 32 Informatica Lookup Transforms to `dbt var()` pattern

**Phase 3: Local Validation & Testing**
- `dbt compile` — Validate SQL syntax, resolve `ref()` and `source()` references
- `dbt run` — Execute all 51 models against Snowflake, create views and tables
- `dbt test` — Run 170 automated tests (not_null, unique, accepted_values, etc.)
- Iterate on failures until 100% pass rate achieved

**Phase 4: Snowflake-Native Deployment**
- Prepare `profiles.yml` for native deployment (no password, Snowflake-managed auth)
- Fix YAML compatibility issues (accepted_values syntax, source deduplication)
- Deploy via `snow dbt deploy` — upload 1,545 files to Snowflake-native project object
- Compile and run directly from Snowflake — 51/51 PASS in 69.29s
- Verify DAG visibility in Snowsight UI

### 4.2 Layered ELT Design Pattern

The dbt project follows the industry-standard **Staging → Intermediate → Marts** pattern:

| Layer | Purpose | Materialization | Naming Convention | Model Count |
|-------|---------|-----------------|-------------------|-------------|
| **Staging** | 1:1 mapping from source tables. Column renaming, type casting, basic cleaning. | `view` | `stg_<source_table>` | 26 |
| **Intermediate** | Business logic, joins, lookups, filtering, aggregation. Multiple staging models combined. | `view` | `int_<business_concept>` | 17 |
| **Marts** | Final fact and dimension tables for consumption. Incremental merge for efficiency. | `incremental` / `table` | `fct_<fact>` / `dim_<dimension>` | 8 |

---

## 5. dbt Project Structure

### 5.1 Directory Layout

```
dbt_project/
├── dbt_project.yml            # Project config: name, vars (32 lookups)
├── profiles.yml               # Snowflake connection (no password for native)
├── packages.yml               # dbt_utils v1.3.0
├── macros/
│   ├── generate_md5_hash.sql  # MD5 hash macro for change detection
│   └── hash_md5_50_fields.sql # Extended hash for wide tables
├── models/
│   ├── m_BL_FF_ZJ_JOURNALS_STG/   # Journal staging pipeline
│   │   ├── staging/                # stg_f0911_jnl_stg, stg_f0901_jnl_stg, ...
│   │   │   ├── _stg__schema.yml
│   │   │   └── _sources.yml       # Consolidated sources (oracle_raw + flat_file_raw)
│   │   ├── intermediate/           # int_journals_base, int_journals_reverse, ...
│   │   │   └── _int__schema.yml
│   │   └── marts/                  # fct_zj_journals_staging, fct_zj_journals_flat_file
│   │       └── _marts__schema.yml
│   ├── m_BL_FF_ZJ_JOURNALS/       # Journal final pipeline
│   ├── m_INCR_DM_DIM_EQUIPMENT/   # Equipment dimension pipeline
│   ├── m_DI_ITEM_MTRL_MASTER/     # Material master pipeline
│   ├── m_AM_DI_CUSTOMER/          # Customer staging
│   ├── m_AP_FF_CITIBANK_VCA/      # Citibank VCA staging
│   ├── m_CM_Z1_DAILY_SALES_FEED/  # Sales feed staging
│   ├── m_FF_AT_Z1_GL_ACCOUNT/     # GL Account Z1 staging
│   └── m_FF_AT_ZJ_GL_ACCOUNT/     # GL Account ZJ staging
├── seeds/
├── snapshots/
├── tests/
└── analyses/
```

### 5.2 Project Configuration (`dbt_project.yml`)

```yaml
name: informatica_to_dbt_migration
version: 1.0.0
profile: informatica_to_dbt_migration
model-paths: [models]
vars:
  lkp_Override_IBP_SCOPE_IND: "SELECT KEY_OF_OBJ, CHAR_VALUE FROM TPC_DI_RAW_DATA.MOCK_SOURCES.LKP_OVERRIDE_CHARS"
  lkp_Override_CE_MARK_IND: "SELECT KEY_OF_OBJ, CHAR_VALUE FROM TPC_DI_RAW_DATA.MOCK_SOURCES.LKP_OVERRIDE_CHARS"
  # ... 30 more lookup variable definitions
```

The 32 `vars` entries represent the conversion of Informatica Lookup Transforms. Each variable holds a SQL subquery that models can reference using `{{ var('lkp_Override_...') }}` to perform lookup joins inline.

### 5.3 Package Dependencies

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

Used for: `accepted_range` tests, `surrogate_key` generation, and utility macros.

---

## 6. Data Flow Architecture

> See diagram: [02_data_flow_diagram.html](diagrams/02_data_flow_diagram.html)

### 6.1 Pipeline: Journals Staging (`m_BL_FF_ZJ_JOURNALS_STG`)

**Sources** → **Staging** → **Intermediate** → **Marts**

```
f0911 (Ledger) ──► stg_f0911_jnl_stg ──┐
f0901 (Acct)   ──► stg_f0901_jnl_stg ──┤
f0011 (Batch)  ──► stg_f0011_jnl_stg ──┼──► int_journals_base ──► int_journals_reverse ──────► fct_zj_journals_staging (INCR)
f0092          ──► stg_f0092 ──────────┤                      └─► int_journals_non_reverse ──► fct_zj_journals_flat_file (TABLE)
f01151         ──► stg_f01151 ─────────┘
```

- **Fact table `fct_zj_journals_staging`**: Incremental model with 5-column composite key. Performs `MERGE` on new records, separating reverse and non-reverse journal entries.
- **Fact table `fct_zj_journals_flat_file`**: Full table rebuild with flat-file journal data.

### 6.2 Pipeline: Equipment Dimension (`m_INCR_DM_DIM_EQUIPMENT`)

```
8 source tables ──► 8 stg_ views ──► int_equipment_aar_base_source ──┐
                                      int_equipment_non_registered ────┤
                                      int_equipment_pool_assignment ──┤
                                      int_equipment_lookups ──────────┼──► int_equipment_final ──► dim_equipment_soft_delete (INCR)
                                      int_dim_equipment_soft_delete ──┘
```

- **Dimension table `dim_equipment_soft_delete`**: Incremental model that performs soft-delete tracking with `EDW_CURRENT_FLG` and `EDW_END_EFCTV_DT` columns.

### 6.3 Pipeline: Material Master (`m_DI_ITEM_MTRL_MASTER`)

```
XML Material ──► stg_xml_material_attr_txt ──► int_material_lookups (32 var lookups) ──► int_material_transformations
                 stg_product_descriptions  ──►   ──► int_material_with_lookups ──► int_material_with_md5 ──► int_material_with_flags
                                                                                                              └──► dim_mtrl_master (INCR)
```

- **Dimension table `dim_mtrl_master`**: Incremental model with 90+ merge-update columns. Uses `insert_update_flag` ('I', 'U') for change data capture.
- This pipeline demonstrates the heaviest Informatica-to-dbt conversion, with 32 lookup variables resolved through `dbt var()`.

---

## 7. Transformation Patterns

### 7.1 Informatica Lookup → dbt var() Pattern

**Before (Informatica):** GUI-based Lookup Transform connecting to a lookup table via cache.

**After (dbt):** SQL subquery stored as a project variable, referenced in models.

```yaml
# dbt_project.yml
vars:
  lkp_Override_IBP_SCOPE_IND: "SELECT KEY_OF_OBJ, CHAR_VALUE FROM TPC_DI_RAW_DATA.MOCK_SOURCES.LKP_OVERRIDE_CHARS"
```

```sql
-- In a model:
LEFT JOIN ({{ var('lkp_Override_IBP_SCOPE_IND') }}) AS lkp_ibp
  ON src.product = lkp_ibp.KEY_OF_OBJ
```

### 7.2 Incremental Processing Pattern

**Before (Informatica):** Full table reload or custom session-level change detection.

**After (dbt):** Native incremental materialization with `MERGE`.

```sql
{{ config(
    materialized='incremental',
    unique_key='DIM_EQUIPMENT_ID',
    merge_update_columns=['EDW_UPDATE_USER', 'EDW_UPDATE_TMS', 'EDW_END_EFCTV_DT', 'EDW_CURRENT_FLG']
) }}

SELECT ... FROM {{ ref('int_dim_equipment_soft_delete') }}

{% if is_incremental() %}
WHERE EDW_UPDATE_TMS > (SELECT MAX(EDW_UPDATE_TMS) FROM {{ this }})
{% endif %}
```

### 7.3 MD5 Change Detection Pattern

**Before (Informatica):** Expression Transform computing hash for change detection.

**After (dbt):** Custom Jinja macro generating `MD5()` across field lists.

```sql
{% macro generate_md5_hash(field_list) %}
    MD5(
        {% for field in field_list %}
            {{ field }}
            {%- if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
```

### 7.4 Router/Filter → UNION ALL Pattern

**Before (Informatica):** Router Transform splitting data into groups based on conditions.

**After (dbt):** CTEs with conditional filters, combined via `UNION ALL`.

```sql
-- Reverse records group
WITH reverse_records AS (
    SELECT ... FROM {{ ref('int_journals_reverse') }}
),
-- Non-reverse records group
non_reverse_records AS (
    SELECT ... FROM {{ ref('int_journals_non_reverse') }}
)
SELECT * FROM reverse_records
UNION ALL
SELECT * FROM non_reverse_records
```

---

## 8. Snowflake-Native Deployment

> See diagram: [04_snowflake_deployment_architecture.html](diagrams/04_snowflake_deployment_architecture.html)

### 8.1 Deployment Architecture

The dbt project is deployed as a **Snowflake-native dbt project object**, not as a local CLI-based project. This means:

- The entire dbt project (1,545 files) is uploaded to a Snowflake internal stage
- Snowflake manages the dbt runtime (dbt-core 1.9.4, snowflake adapter 1.9.2)
- No external compute or CI/CD infrastructure required for execution
- Project is versioned (VERSION$1) and can be updated with new deployments

### 8.2 Snowflake Objects Created

| Object Type | Name | Description |
|-------------|------|-------------|
| **Database** | `TPC_DI_RAW_DATA` | Contains all schemas |
| **Schema** | `MOCK_SOURCES` | 38 source tables (Oracle + flat files) |
| **Schema** | `DBT_INGEST` | 51 dbt-created objects + native project |
| **dbt Project** | `INFORMATICA_TO_DBT` | Native project object (VERSION$1) |
| **Warehouse** | `SMALL_WH` | Compute warehouse (Small, auto-suspend 60s) |
| **Views** | 44 views | Staging + intermediate layer models |
| **Tables** | 3 tables | Fact tables (full rebuild) |
| **Incremental Tables** | 4 tables | Fact + dimension tables (merge) |

### 8.3 Execution Methods

**Method 1: Snowflake CLI**
```bash
# Compile
snow dbt execute -c myconnection --database TPC_DI_RAW_DATA --schema DBT_INGEST INFORMATICA_TO_DBT compile

# Run
snow dbt execute -c myconnection --database TPC_DI_RAW_DATA --schema DBT_INGEST INFORMATICA_TO_DBT run

# Test
snow dbt execute -c myconnection --database TPC_DI_RAW_DATA --schema DBT_INGEST INFORMATICA_TO_DBT test

# Full refresh (rebuild incremental tables from scratch)
snow dbt execute -c myconnection --database TPC_DI_RAW_DATA --schema DBT_INGEST INFORMATICA_TO_DBT run --args='--full-refresh'
```

**Method 2: SQL (from Snowsight or any SQL client)**
```sql
-- Run all models
EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run';

-- Run tests
EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'test';

-- Compile only
EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'compile';
```

**Method 3: Snowsight UI**
1. Navigate to **Data** → **Databases** → **TPC_DI_RAW_DATA** → **DBT_INGEST** → **dbt Projects** → **INFORMATICA_TO_DBT**
2. Use built-in controls to compile, run, and test
3. View the DAG (dependency graph) for visual lineage

### 8.4 Scheduling (Production Readiness)

For production use, the dbt project can be scheduled using Snowflake Tasks:

```sql
CREATE TASK dbt_nightly_run
  WAREHOUSE = SMALL_WH
  SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
  EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run';
```

---

## 9. Testing Strategy

### 9.1 Test Coverage Summary

| Test Type | Count | Examples |
|-----------|-------|---------|
| `not_null` | ~60 | Primary keys, required business fields |
| `unique` | ~30 | Primary keys, natural keys |
| `accepted_values` | ~40 | Status codes ('Y'/'N'), type codes ('I'/'U'/'E') |
| `relationships` | ~20 | Foreign key integrity between models |
| `dbt_utils.accepted_range` | ~20 | Numeric range validation (amounts, quantities) |
| **Total** | **170** | |

### 9.2 Test Example

```yaml
version: 2
models:
  - name: fct_zj_journals_staging
    columns:
      - name: f0911_gldct
        tests:
          - not_null
          - unique
      - name: f0911_glpost
        tests:
          - accepted_values:
              values: ['P']
```

---

## 10. Key Design Decisions

### 10.1 Lookup Conversion Strategy

**Decision:** Convert Informatica Lookup Transforms to `dbt var()` with SQL subqueries.

**Rationale:** Informatica lookups are essentially cached SELECT statements. By storing the SQL as dbt project variables, models can reference them via `LEFT JOIN ({{ var('lkp_...') }})`. This preserves the lookup semantics while making them transparent, testable, and version-controlled.

**Trade-off:** 32 variables in `dbt_project.yml` adds configuration complexity, but ensures each lookup is explicitly documented and can be independently modified.

### 10.2 Materialization Strategy

**Decision:** Use views for staging/intermediate, incremental for dimension/fact marts.

| Model Type | Materialization | Reason |
|-----------|-----------------|--------|
| Staging (26) | `view` | Lightweight, always reflects latest source data |
| Intermediate (17) | `view` | Business logic layer, no storage cost |
| Fact tables (3) | `table` | Full rebuild for non-incremental fact data |
| Incremental (4) | `incremental` | Efficient MERGE for large dimension/fact tables |

### 10.3 Source Consolidation

**Decision:** Single consolidated `_sources.yml` file for all sources.

**Rationale:** Snowflake-native dbt runtime requires unique source names across the entire project. Multiple files defining `oracle_raw` caused deployment failures. Consolidating into one file in `m_BL_FF_ZJ_JOURNALS_STG/staging/_sources.yml` resolves this while maintaining all 38 table definitions.

### 10.4 Native vs Local Deployment

**Decision:** Deploy as Snowflake-native dbt project (not local CLI only).

**Rationale:**
- Enables execution directly from Snowsight UI (no local tooling required)
- Provides visual DAG in the Snowflake workspace
- Supports scheduling via Snowflake Tasks
- Eliminates dependency on developer machines for production runs
- Versioned deployments (VERSION$1, VERSION$2, etc.)

---

## 11. Deployment Results

### 11.1 Compilation Results

```
Running with dbt=1.9.4
Registered adapter: snowflake=1.9.2
Found 51 models, 1 snapshot, 170 data tests, 38 sources, 592 macros
```

### 11.2 Execution Results

```
Finished running 4 incremental models, 3 table models, 44 view models
  in 0 hours 1 minutes and 9.29 seconds (69.29s).

Completed successfully
Done. PASS=51 WARN=0 ERROR=0 SKIP=0 TOTAL=51
```

### 11.3 Object Inventory

| Object | Count | Materialization |
|--------|-------|-----------------|
| Views | 44 | `view` |
| Tables | 3 | `table` |
| Incremental Tables | 4 | `incremental` (MERGE) |
| **Total Models** | **51** | |
| Data Tests | 170 | |
| Sources | 38 | |
| Macros | 592 (2 custom + 590 from packages) | |

---

## 12. Operational Runbook

### 12.1 Daily Operations

| Task | Command | Frequency |
|------|---------|-----------|
| Run all models | `EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run';` | Daily |
| Run tests | `EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'test';` | After each run |
| Full refresh (incremental rebuild) | `EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run --full-refresh';` | As needed |

### 12.2 Updating the Project

When models need to be modified:

1. Edit model SQL files locally in `dbt_project/models/`
2. Test locally: `dbt compile && dbt run && dbt test`
3. Redeploy to Snowflake:
   ```bash
   snow dbt deploy INFORMATICA_TO_DBT \
     --source /path/to/dbt_project \
     --database TPC_DI_RAW_DATA \
     --schema DBT_INGEST \
     -c myconnection
   ```
4. Verify: `SHOW VERSIONS IN DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT;`

### 12.3 Monitoring

- **Snowsight**: Navigate to the dbt project object to view run history, logs, and DAG
- **Query History**: All dbt model executions appear in `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- **Alerts**: Configure Snowflake Alerts on task failure for scheduled runs

---

## 13. Generic Framework Architecture

The migration framework is designed as a **generic, reusable CLI tool** (`infa2dbt`) that converts any Informatica PowerCenter XML exports into production-ready dbt projects. Unlike rule-based tools, it uses **Snowflake Cortex LLM** for intelligent code generation with a self-healing validation loop.

### 13.1 Framework Module Map

```
informatica_to_dbt/
├── cli.py                          # Click CLI — 7 commands (convert, discover, cache, validate, git-push, deploy, version)
├── config.py                       # Central configuration (database, schema, LLM model, cache, Git)
├── orchestrator.py                 # Main pipeline: XML → analyze → chunk → LLM → validate → output
├── metrics.py                      # Per-mapping and repository-level metrics collection
├── exceptions.py                   # Error hierarchy with 6 categories (INPUT, LLM, PARSE, VALIDATION, PERSISTENCE, INTERNAL)
│
├── xml_parser/                     # Phase 0: Parse Informatica PowerCenter XML
│   ├── parser.py                   #   XML → Repository model (mappings, transforms, sources, targets)
│   ├── models.py                   #   Data classes: Repository, Workflow, Mapping, Transformation, etc.
│   └── dependency_graph.py         #   DAG extraction from XML transformation links
│
├── analyzer/                       # Phase 1: Complexity analysis & strategy selection
│   ├── complexity.py               #   11-dimension scoring (0-100) → DIRECT/STAGED/LAYERED/COMPLEX
│   ├── multi_workflow.py           #   Multi-workflow enrichment with cross-mapping metadata
│   └── transformation_registry.py  #   30+ Informatica transform types → dbt pattern mapping
│
├── chunker/                        # Phase 2: Token-aware chunking for large mappings
│   └── context_preserving.py       #   Split while preserving transformation chains (128K limit)
│
├── generator/                      # Phase 3: LLM-powered dbt code generation
│   ├── llm_client.py              #   Snowflake Cortex LLM calls (claude-4-sonnet)
│   ├── prompt_builder.py          #   Dynamic prompt construction with schema + complexity context
│   ├── response_parser.py         #   Extract SQL/YAML files from LLM response
│   ├── post_processor.py          #   Fix common LLM output issues (refs, naming, formatting)
│   ├── quality_scorer.py          #   Score generated code quality (0-100)
│   └── dbt_project_generator.py   #   Generate dbt_project.yml, profiles.yml, packages.yml
│
├── validator/                      # Phase 4a: Static validation (no Snowflake needed)
│   ├── sql_validator.py           #   SQL syntax and ref/source validation
│   ├── yaml_validator.py          #   YAML structure and schema validation
│   └── project_validator.py       #   dbt project structure validation
│
├── validation/                     # Phase 4b: Live validation (requires Snowflake)
│   └── dbt_validator.py           #   dbt compile / dbt run / dbt test subprocess wrapper
│
├── cache/                          # Deterministic output caching
│   └── conversion_cache.py        #   SHA-256 fingerprint → cached LLM output (skip re-generation)
│
├── discovery/                      # Pre-conversion inventory & schema discovery
│   ├── xml_inventory.py           #   Recursive XML file scanning with metadata extraction
│   └── schema_discovery.py        #   3 modes: Snowflake INFORMATION_SCHEMA, XML, manual JSON
│
├── merger/                         # Multi-mapping project assembly
│   ├── project_merger.py          #   Merge multiple mapping outputs into one dbt project
│   ├── source_consolidator.py     #   Deduplicate source definitions across mappings
│   └── conflict_resolver.py       #   Handle naming conflicts between mappings
│
├── git/                            # Version control integration
│   └── git_manager.py            #   Git init/branch/commit/push/remote management
│
├── deployment/                     # Snowflake deployment
│   └── deployer.py               #   3 modes: direct (snow dbt deploy), Git-based, TASK scheduling
│
├── reports/                        # Assessment reporting
│   └── ewi_report.py             #   EWI (Errors/Warnings/Informational) HTML + JSON reports
│
└── persistence/                    # Snowflake I/O (stage read/write, table persistence)
    └── snowflake_io.py            #   Read XML from stages, write results to tables
```

### 13.2 Pipeline Flow

See diagram: [06_framework_pipeline.html](diagrams/06_framework_pipeline.html)

The end-to-end pipeline executes in 7 stages:

| Stage | CLI Command | Input | Output |
|-------|-------------|-------|--------|
| 1. Discovery | `infa2dbt discover` | XML directory | Inventory + schema metadata |
| 2. Analysis | `infa2dbt convert` (phase 1) | Parsed XML | Complexity scores, strategies |
| 3. Conversion | `infa2dbt convert` (phase 2) | Enriched mappings | dbt SQL, YAML, macros |
| 4. Assembly | `infa2dbt convert --mode merge` | Per-mapping files | Unified dbt project |
| 5. Validation | `infa2dbt validate` | dbt project | Compile/run/test results |
| 6. Git Push | `infa2dbt git-push` | Validated project | Git commit + push |
| 7. Deploy | `infa2dbt deploy` | Local or Git project | Snowflake dbt project object |

### 13.3 CLI Command Reference

See full documentation: [CLI_Reference.md](CLI_Reference.md)

| Command | Description |
|---------|-------------|
| `infa2dbt convert` | Convert Informatica XML to dbt (new or merge mode) |
| `infa2dbt discover` | Inventory XML files and discover source schemas |
| `infa2dbt cache` | Manage conversion output cache (list, clear, stats) |
| `infa2dbt validate` | Validate dbt project via compile/run/test |
| `infa2dbt git-push` | Commit and push to Git repository |
| `infa2dbt deploy` | Deploy to Snowflake (direct, Git-based, or scheduled) |
| `infa2dbt version` | Show version information |

### 13.4 Test Coverage

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `test_xml_parser.py` | 13 | XML parsing, model extraction |
| `test_validators.py` | 23 | SQL, YAML, project validation |
| `test_phase7.py` | 42 | Metrics enrichment, error categorization |
| `test_merger.py` | 23 | Project merge, source consolidation, conflicts |
| `test_cache.py` | 23 | Cache keys, store/retrieve, disabled mode |
| `test_discovery.py` | 17 | XML inventory, schema discovery |
| `test_dbt_validator.py` | 21 | dbt compile/run/test subprocess |
| `test_git_manager.py` | 23 | Git init/commit/push/remote |
| `test_deployer.py` | 23 | Direct/Git/schedule deployment |
| `test_ewi_report.py` | 21 | EWI classification, HTML/JSON output |
| `test_merge_pipeline.py` | 121 | Integration: multi-mapping merge pipeline |
| **Total** | **350** | |

---

## 14. 30-Minute Demo Plan

### Demo Objective
Demonstrate the end-to-end Informatica-to-dbt migration framework, showing the conversion process, project structure, Snowflake-native deployment, and live execution.

### Prerequisites
- Snowsight access to account `EPCKZEL-SQUADRON_US_EAST_1`
- Browser with HTML rendering (for diagram files)
- Terminal with `snow` CLI configured

### Demo Script

---

#### Segment 1: Context & Architecture (5 minutes)

**Goal:** Set the stage. Explain why the migration was done and what the end state looks like.

1. **Open** [05_before_after_comparison.html](diagrams/05_before_after_comparison.html) in browser
   - Walk through the "Before" side: Informatica pain points (licensing, no Git, no tests)
   - Walk through the "After" side: dbt benefits (serverless, tests, lineage)

2. **Open** [01_high_level_architecture.html](diagrams/01_high_level_architecture.html) in browser
   - Trace the flow: Sources → Conversion Engine → dbt Project → Snowflake → Outputs
   - Highlight: 9 mappings → 51 models, 32 lookups → var(), 170 tests

---

#### Segment 2: dbt Project Walkthrough (7 minutes)

**Goal:** Show the actual code structure and explain the layered design.

3. **Open terminal**, navigate to project:
   ```bash
   ls /Users/vicky/informatica-to-dbt/dbt_project/
   ```
   - Show `dbt_project.yml` — point out the 32 lookup variables
   - Show `packages.yml` — dbt_utils dependency
   - Show `profiles.yml` — Snowflake connection (no password for native)

4. **Show model directories:**
   ```bash
   ls models/
   ```
   - Explain naming: `m_<INFORMATICA_MAPPING_NAME>`
   - Each mapping has `staging/`, `intermediate/`, `marts/` subdirectories

5. **Open** [02_data_flow_diagram.html](diagrams/02_data_flow_diagram.html) in browser
   - Walk through one pipeline (Journals STG) end to end
   - Show how 5 source tables flow through staging → intermediate → 2 mart tables

6. **Show a sample model** — open `models/m_INCR_DM_DIM_EQUIPMENT/marts/dim_equipment_soft_delete.sql`:
   - Point out `{{ config(materialized='incremental', unique_key='DIM_EQUIPMENT_ID') }}`
   - Point out `{{ ref('int_dim_equipment_soft_delete') }}` — model dependency
   - Point out `{% if is_incremental() %}` — only new records processed

---

#### Segment 3: Snowflake-Native Deployment (5 minutes)

**Goal:** Show that the project exists as a first-class Snowflake object.

7. **Open** [04_snowflake_deployment_architecture.html](diagrams/04_snowflake_deployment_architecture.html) in browser
   - Explain: dbt project is an object inside Snowflake, not running externally

8. **In Snowsight**, navigate to:
   - **Data** → **Databases** → **TPC_DI_RAW_DATA** → **DBT_INGEST**
   - Show the **dbt Projects** section → **INFORMATICA_TO_DBT**
   - Show VERSION$1

9. **Show deployed objects** — click through the schema to show views and tables:
   - Point out `stg_` views (staging layer)
   - Point out `int_` views (intermediate layer)
   - Point out `fct_` and `dim_` tables (marts layer)

---

#### Segment 4: Live Execution (8 minutes)

**Goal:** Run the project live from Snowflake and show results.

10. **Run compile from SQL worksheet:**
    ```sql
    EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'compile';
    ```
    - Show output: 51 models, 170 tests, 38 sources found

11. **Run the project:**
    ```sql
    EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'run';
    ```
    - Watch models execute in real-time
    - Point out the execution order (staging first, then intermediate, then marts)
    - Show final result: **51/51 PASS**

12. **Query a result table:**
    ```sql
    SELECT COUNT(*) FROM TPC_DI_RAW_DATA.DBT_INGEST.fct_zj_journals_staging;
    SELECT * FROM TPC_DI_RAW_DATA.DBT_INGEST.dim_mtrl_master LIMIT 10;
    ```
    - Show actual data flowing through the pipeline

13. **Show the DAG:**
    - Navigate to the dbt project in Snowsight
    - Click on the **Lineage / DAG** view
    - Trace a pipeline from source to final mart table

---

#### Segment 5: Testing & Quality (3 minutes)

14. **Run tests:**
    ```sql
    EXECUTE DBT PROJECT TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT ARGS = 'test';
    ```
    - Show 170 tests executing
    - Explain test types: not_null, unique, accepted_values, relationships

---

#### Segment 6: Summary & Q&A (2 minutes)

15. **Open** [03_migration_process_flow.html](diagrams/03_migration_process_flow.html) in browser
    - Recap the 4-phase methodology
    - Highlight the results banner: 9 mappings → 51 models, 170 tests, 100% pass rate, 0 errors

16. **Key takeaways:**
    - Informatica PowerCenter decommissioned — no more licensing cost
    - All pipelines running natively in Snowflake — no external infrastructure
    - 170 automated tests ensure data quality with every run
    - Full DAG lineage visible in Snowsight
    - SQL-first, Git-friendly, version-controlled codebase

17. **Open for Q&A**

---

### Demo Timing Summary

| Segment | Duration | Content |
|---------|----------|---------|
| 1. Context & Architecture | 5 min | Before/After diagram, High-level architecture |
| 2. Project Walkthrough | 7 min | Code structure, data flow diagram, sample model |
| 3. Snowflake Deployment | 5 min | Deployment architecture, Snowsight navigation |
| 4. Live Execution | 8 min | Compile, run, query results, DAG viewer |
| 5. Testing & Quality | 3 min | Run tests, explain test types |
| 6. Summary & Q&A | 2 min | Process flow recap, key takeaways |
| **Total** | **30 min** | |

---

*Document generated for the Informatica to dbt Migration Framework project.*
*Deployed to: `TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_TO_DBT` (VERSION$1)*
*Snowflake Account: EPCKZEL-SQUADRON_US_EAST_1*
