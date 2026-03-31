"""Build LLM prompts for Informatica-to-dbt conversion.

Constructs a system prompt with dbt best practices and Snowflake SQL
conventions, then a user prompt containing the serialised mapping chunk
and strategy-specific instructions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from informatica_to_dbt.analyzer.complexity import ComplexityResult, ModelStrategy
from informatica_to_dbt.analyzer.transformation_registry import (
    get_transformation_type,
    get_conversion_hint,
    FUNCTION_MAP,
)
from informatica_to_dbt.chunker.context_preserving import MappingChunk

logger = logging.getLogger("informatica_dbt")

# ---------------------------------------------------------------------------
# System prompt (shared across all calls)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert data engineer converting Informatica PowerCenter ETL \
mappings into dbt (data build tool) projects targeting Snowflake.

## Output Rules
1. Return ONLY valid file blocks — no commentary outside code fences.
2. Every file block MUST start with: `-- FILE: <relative_path>`
   Example: `-- FILE: models/staging/stg_citibank_vca.sql`
3. SQL files use Snowflake SQL dialect (VARIANT, FLATTEN, TRY_CAST, etc.).
4. YAML files use dbt schema.yml / sources.yml conventions.
5. Use `{{ ref('model_name') }}` for inter-model references.
6. Use `{{ source('source_name', 'table_name') }}` for raw sources.
7. Follow the dbt naming convention:
   - staging: `stg_<source>__<table>.sql`
   - intermediate: `int_<description>.sql`
   - marts: `fct_` or `dim_<description>.sql`
8. Include a `schema.yml` with column descriptions and tests for every \
model layer you generate.

## Transformation Type Conversion Rules

### Source Qualifiers
9. Source Qualifier / App Multi-Group Source Qualifier / XML Source Qualifier:
   - If `Sql Query` override attribute exists, use it as the base SELECT.
   - Otherwise generate SELECT from source columns via `source()`.
   - Apply `Source Filter` as a WHERE clause.
   - For XML Source Qualifier, use PARSE_XML / XMLGET or LATERAL FLATTEN.

### Expression Transformations
10. Expression → SQL column expressions:
    - Each OUTPUT port expression becomes a column expression in SELECT.
    - Apply the function conversion rules below for all Informatica functions.
    - LOCAL VARIABLE ports are intermediate calculations — use CTEs or subexpressions.

### Lookup Procedure
11. Lookup Procedure → LEFT JOIN or CTE:
    - Convert to LEFT JOIN using the `Lookup condition` attribute.
    - If `Lookup Sql Override` exists, wrap it as a CTE and join to that.
    - Respect `Lookup Policy on Multiple Match` — use QUALIFY ROW_NUMBER()=1 \
if policy is "Use First/Last Value".
    - Unconnected lookups (`:LKP.name(key)`) → correlated subquery or LEFT JOIN.

### Router / Filter
12. Router → CASE WHEN or separate CTEs:
    - Each Router GROUP becomes a CASE WHEN branch or a separate CTE \
with the group's filter condition.
    - The DEFAULT group captures rows not matching any other group condition.
13. Filter → WHERE clause:
    - Convert `Filter Condition` attribute to a WHERE clause.

### Joiner
14. Joiner → SQL JOIN:
    - `Join Type` mapping: Normal→INNER JOIN, Master Outer→LEFT JOIN, \
Detail Outer→RIGHT JOIN, Full Outer→FULL OUTER JOIN.
    - Convert `Join Condition` attribute to the ON clause.
    - If `Sorted Input` = YES, add ORDER BY hint as a comment.

### Sorter
15. Sorter → ORDER BY (when downstream logic depends on order):
    - In dbt/Snowflake, sorting is generally implicit in final output.
    - If `Distinct` = YES on the Sorter, add SELECT DISTINCT.
    - Use ORDER BY in window functions when downstream relies on row order.

### Aggregator
16. Aggregator → GROUP BY:
    - Group-by ports become GROUP BY columns.
    - Output ports with aggregate expressions map to SUM/COUNT/MIN/MAX/AVG.
    - Translate nested Informatica functions within aggregates.

### Rank
17. Rank → ROW_NUMBER / RANK window function:
    - Convert to ROW_NUMBER() OVER (PARTITION BY group_ports ORDER BY rank_port).
    - Filter to ≤ N based on `Number of Ranks` attribute.
    - `Top/Bottom` determines ASC/DESC ordering.

### Normalizer
18. Normalizer → UNPIVOT or LATERAL FLATTEN:
    - One-to-many row generation maps to UNPIVOT or LATERAL FLATTEN.
    - GENERATED_COLUMN_ID maps to a generated row index.

### Union / Custom Transformation
19. Union → UNION ALL:
    - Each input group becomes a SELECT in a UNION ALL.
    - Ensure column alignment and type compatibility between groups.

### Update Strategy
20. Update Strategy → dbt incremental strategy:
    - DD_INSERT → standard INSERT (append).
    - DD_UPDATE → `merge` with `unique_key`.
    - Combined INSERT/UPDATE → `incremental` with `merge` strategy.
    - SCD Type 2 patterns → dbt `snapshot` or incremental with surrogate key.
    - Dual-target patterns (Insert_X / Update_X) → single incremental model \
with MERGE.

### Sequence Generator
21. Sequence Generator → ROW_NUMBER() or dbt_utils.generate_surrogate_key():
    - For surrogate keys, prefer dbt_utils.generate_surrogate_key().
    - For sequential IDs, use ROW_NUMBER() OVER (ORDER BY ...).

### Mapplet
22. Mapplet → dbt macro or CTE:
    - Reusable mapplet logic becomes a dbt macro.
    - Input Transformation ports map to macro parameters.
    - Output Transformation ports map to macro return columns.
    - For simple mapplets (e.g., MD5 hashing), inline as a CTE.

### Transaction Control / Stored Procedure
23. Transaction Control → document as comments; use post-hooks if critical.
24. Stored Procedure → dbt run-operation macro or Snowflake stored procedure.

## Informatica → Snowflake Function Conversion Rules

### Conditional Functions
25. `IIF(cond, true_val, false_val)` → `IFF(cond, true_val, false_val)`
26. `DECODE(TRUE, c1, v1, c2, v2, default)` → \
`CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE default END`
27. Simple `DECODE(col, v1, r1, v2, r2, default)` → Snowflake `DECODE()` directly.

### Null Handling
28. `ISNULL(x)` → `x IS NULL` (in boolean context) or `COALESCE(x, default)`.
29. `NVL(a, b)` → `NVL(a, b)` or `COALESCE(a, b)`.
30. `NVL2(expr, not_null, null_val)` → `NVL2(expr, not_null, null_val)`.

### String Functions
31. `LTRIM(RTRIM(x))` → `TRIM(x)` (or keep LTRIM/RTRIM individually).
32. `REPLACESTR(1, str, old, new)` → `REPLACE(str, old, new)`.
33. `REPLACECHR(0, str, chars, repl)` → `TRANSLATE(str, chars, repl)`. \
If repl is NULL, use REPLACE for each char individually.
34. `REG_EXTRACT(str, pat, grp)` → `REGEXP_SUBSTR(str, pat, 1, 1, 'e', grp)`.
35. `REG_REPLACE(str, pat, repl)` → `REGEXP_REPLACE(str, pat, repl)`.
36. `REG_MATCH(str, pat)` → `REGEXP_LIKE(str, pat)` or `RLIKE(str, pat)`.
37. `CHR(n)` → `CHR(n)` (direct equivalent, e.g., CHR(34) = double quote).
38. `CONCAT(a, b, ...)` → `CONCAT(a, b)` or `a || b || ...`.
39. `INSTR(str, sub)` → `POSITION(sub IN str)` or `CHARINDEX(sub, str)`.

### Numeric / Type Conversion
40. `TRUNC(num, scale)` → `TRUNC(num, scale)` (direct equivalent).
41. `TO_CHAR(date, 'YYYYMMDD')` → `TO_CHAR(date, 'YYYYMMDD')`.
42. `TO_CHAR(TO_DECIMAL(x, scale))` → `TO_VARCHAR(TO_DECIMAL(x, 38, scale))`.
43. `TO_DECIMAL(str, scale)` → `TO_DECIMAL(str, 38, scale)` or `TO_NUMBER()`.
44. `TO_INTEGER(x)` → `TO_NUMBER(x, 38, 0)`.
45. `TO_DATE(str, fmt)` → `TRY_TO_DATE(str, fmt)`.

### Date / Time
46. `SYSDATE` → `CURRENT_TIMESTAMP()`.
47. `SESSSTARTTIME` → `CURRENT_TIMESTAMP()`.
48. `ADD_TO_DATE(date, 'MM', n)` → `DATEADD('MONTH', n, date)`.
49. `DATE_DIFF(d1, d2, 'D')` → `DATEDIFF('DAY', d2, d1)`.
50. `GET_DATE_PART(part, date)` → `DATE_PART(part, date)`.
51. `LAST_DAY(date)` → `LAST_DAY(date)`.
52. `TRUNC(date, 'MM')` → `DATE_TRUNC('MONTH', date)`.

### Hash / Special
53. `MD5(str)` → `MD5(str)` (direct equivalent).
54. `ERROR('msg')` as DEFAULTVALUE → omit or convert to a dbt test assertion.
55. `ABORT('msg')` → convert to a dbt test or macro assertion.
56. `$$PARAM` → `{{ var('param') }}` or `{{ env_var('PARAM') }}`.
57. `$PMmapping.param` → `{{ var('param') }}`.

## Quality Standards
- Generated SQL must parse without errors on Snowflake.
- No hardcoded environment-specific values (use dbt vars or env_var).
- Include appropriate dbt tests: unique, not_null, accepted_values, \
relationships where applicable.
- Add `{{ config(materialized='...') }}` at the top of each model.
- NEVER leave Informatica function names in the output SQL. Every IIF, \
ISNULL, REPLACESTR, REPLACECHR, DECODE(TRUE,...), SESSSTARTTIME, etc. \
MUST be converted to the Snowflake equivalent listed above.

## Few-Shot Examples

### Example 1: Expression with Lookup (STAGED strategy)
Input Informatica snippet:
```
TRANSFORMATION: SQ_T_CM (type=Source Qualifier)
TRANSFORMATION: EXP_TRANSFORM (type=Expression)
  OUT_FULL_NAME (OUTPUT): IIF(ISNULL(LTRIM(RTRIM(FIRST_NAME))), LAST_NAME, \
LTRIM(RTRIM(FIRST_NAME)) || ' ' || LAST_NAME)
  OUT_STATUS (OUTPUT): DECODE(TRUE, STATUS_CD='A', 'Active', \
STATUS_CD='I', 'Inactive', 'Unknown')
TRANSFORMATION: LKP_REGION (type=Lookup Procedure)
  Lookup condition: REGION_ID = IN_REGION_ID
  Lookup Sql Override: SELECT REGION_ID, REGION_NAME FROM DIM_REGION
```

Expected dbt output:
```sql
-- FILE: models/staging/stg_t_cm.sql
{{ config(materialized='view') }}

SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    STATUS_CD,
    REGION_ID
FROM {{ source('oracle_raw', 't_cm') }}

-- FILE: models/intermediate/int_t_cm_transformed.sql
{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_t_cm') }}
),

lkp_region AS (
    SELECT REGION_ID, REGION_NAME FROM {{ ref('stg_dim_region') }}
)

SELECT
    s.CUSTOMER_ID,
    IFF(
        s.FIRST_NAME IS NULL OR TRIM(s.FIRST_NAME) = '',
        s.LAST_NAME,
        TRIM(s.FIRST_NAME) || ' ' || s.LAST_NAME
    ) AS FULL_NAME,
    CASE
        WHEN s.STATUS_CD = 'A' THEN 'Active'
        WHEN s.STATUS_CD = 'I' THEN 'Inactive'
        ELSE 'Unknown'
    END AS STATUS,
    r.REGION_NAME
FROM source_data s
LEFT JOIN lkp_region r
    ON s.REGION_ID = r.REGION_ID
```

### Example 2: Router with Update Strategy (COMPLEX strategy)
Input: Router groups + Update Strategy with DD_INSERT/DD_UPDATE
Expected: Incremental model with merge strategy, CASE WHEN for router groups.
```sql
-- FILE: models/marts/dim_customer.sql
{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_KEY',
    merge_update_columns=['CUSTOMER_NAME', 'STATUS', 'UPDATED_AT']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} AS CUSTOMER_KEY,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    STATUS,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM {{ ref('int_customer_transformed') }}
{% if is_incremental() %}
WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
```

### Example 3: Flat File Source
Input: Source with database_type=Flat File, FLATFILE definition.
Expected: Use `source()` pointing to a staged external table or COPY INTO pattern.
```sql
{{ config(materialized='view') }}
SELECT * FROM {{ source('flat_file_raw', 'customer_feed') }}
```
"""


# ---------------------------------------------------------------------------
# Strategy-specific instructions
# ---------------------------------------------------------------------------

_STRATEGY_INSTRUCTIONS = {
    ModelStrategy.DIRECT: """\
## Strategy: DIRECT (simple mapping)
Generate a SINGLE staging model that directly translates the source-to-target \
transformation. Include a sources.yml and a schema.yml.
- Inline all expression logic directly in the SELECT columns.
- Convert ALL Informatica functions to Snowflake equivalents in the SELECT.
- Use source() for the FROM clause — never hardcode table names.
Output files:
- models/staging/stg_<name>.sql
- models/staging/_sources.yml
- models/staging/_stg__schema.yml
""",
    ModelStrategy.STAGED: """\
## Strategy: STAGED (moderate complexity)
Generate TWO layers:
1. A staging model (`stg_`) with source selection and type casting.
2. An intermediate model (`int_`) with business logic and transformations.
Include sources.yml and schema.yml for each layer.
- Staging model: SELECT columns with CAST/type conversions, column renaming. \
Use source() for FROM clause.
- Intermediate model: Use ref('stg_...') and apply all expression logic, \
lookup JOINs (convert LKP to LEFT JOIN on ref()), filter conditions.
- Convert ALL Informatica functions inline — no unconverted functions allowed.
- Lookups become CTEs or JOINs on ref() models; never use :LKP syntax.
Output files:
- models/staging/stg_<name>.sql
- models/staging/_sources.yml
- models/staging/_stg__schema.yml
- models/intermediate/int_<name>.sql
- models/intermediate/_int__schema.yml
""",
    ModelStrategy.LAYERED: """\
## Strategy: LAYERED (high complexity)
Generate THREE layers:
1. Staging (`stg_`): raw source selection, type casting, column renaming.
2. Intermediate (`int_`): business logic, lookups as JOINs, expression transforms.
3. Marts (`dim_` or `fct_`): final target model with all transformations applied.
Include sources.yml and schema.yml for each layer.
Output files:
- models/staging/stg_<name>.sql  (one per source table)
- models/staging/_sources.yml
- models/staging/_stg__schema.yml
- models/intermediate/int_<name>.sql
- models/intermediate/_int__schema.yml
- models/marts/dim_<name>.sql  OR  models/marts/fct_<name>.sql
- models/marts/_marts__schema.yml
""",
    ModelStrategy.COMPLEX: """\
## Strategy: COMPLEX (very high complexity — SCD Type 2, Router, Union, many lookups)
Generate a FULL dbt project structure:
1. Staging: one model per source table with type casting.
2. Intermediate: one or more models for lookup JOINs, union operations, \
expression transforms, and routing logic.
3. Marts: final dimension or fact model implementing update strategy \
(incremental merge or snapshot for SCD Type 2).
4. If SCD Type 2 is detected, generate a dbt snapshot model.
5. Create dbt macros for any reusable transformation logic (e.g., MD5 hashing).
Include sources.yml and schema.yml for every layer.
Key conversion rules for COMPLEX mappings:
- Router groups → CASE WHEN conditions or UNION ALL of filtered CTEs.
- Update Strategy DD_INSERT → standard INSERT (materialized='incremental').
- Update Strategy DD_UPDATE → MERGE via incremental + unique_key.
- Update Strategy DD_DELETE → soft-delete flag column or separate delete model.
- Sequence Generator NEXTVAL → ROW_NUMBER() or identity column.
- Joiner → appropriate JOIN type (INNER/LEFT/FULL OUTER) based on join type attr.
- Sorter → ORDER BY in the appropriate CTE or window function.
- Mapplet → inline the mapplet logic or create a reusable dbt macro.
- Multiple sources → one stg_ model per source, joined in intermediate layer.
Output files:
- models/staging/stg_<source>.sql  (multiple)
- models/staging/_sources.yml
- models/staging/_stg__schema.yml
- models/intermediate/int_<step>.sql  (multiple)
- models/intermediate/_int__schema.yml
- models/marts/dim_<name>.sql  OR  snapshots/snap_<name>.sql
- models/marts/_marts__schema.yml
- macros/<reusable_logic>.sql  (if applicable)
""",
}

_MULTI_CHUNK_NOTE = """\
## Multi-Chunk Context
This mapping has been split across {total} chunks due to size. This is \
chunk {index} of {total}. Generate dbt code ONLY for the transformations \
included in this chunk. The final assembly will merge all chunks. Ensure \
ref() calls are consistent across chunks.
"""


# ---------------------------------------------------------------------------
# Prompt dataclass
# ---------------------------------------------------------------------------

@dataclass
class PromptPair:
    """System + user prompt ready for LLM submission."""

    system: str
    user: str


def _build_type_hints(complexity: ComplexityResult) -> str:
    """Build dynamic conversion hints for the transformation types in this mapping."""
    hints = []
    for type_name in complexity.transformation_type_counts:
        hint = get_conversion_hint(type_name)
        if hint:
            count = complexity.transformation_type_counts[type_name]
            hints.append(f"- **{type_name}** (×{count}): {hint}")
    if not hints:
        return ""
    return "## Type-Specific Conversion Hints\n" + "\n".join(hints) + "\n"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_prompt(
    chunk: MappingChunk,
    complexity: ComplexityResult,
    extra_context: Optional[str] = None,
) -> PromptPair:
    """Build the system and user prompts for a single chunk.

    Args:
        chunk: Serialised mapping content.
        complexity: Complexity analysis result (determines strategy instructions).
        extra_context: Optional additional instructions appended to user prompt.
    """
    strategy = complexity.strategy
    strategy_instructions = _STRATEGY_INSTRUCTIONS.get(strategy, "")

    user_parts = [strategy_instructions]

    if chunk.total_chunks > 1:
        user_parts.append(
            _MULTI_CHUNK_NOTE.format(
                index=chunk.chunk_index + 1,
                total=chunk.total_chunks,
            )
        )

    user_parts.append(
        f"## Complexity Analysis\n"
        f"- Score: {complexity.score}/100\n"
        f"- Transformation types: {complexity.transformation_type_counts}\n"
        f"- Sources: {complexity.num_sources}, Targets: {complexity.num_targets}\n"
        f"- Chains: {complexity.num_chains}, Longest path: {complexity.longest_path}\n"
        f"- SCD Type 2 detected: {complexity.has_scd2}\n"
    )

    # Dynamic type-specific hints based on what's in this mapping
    type_hints = _build_type_hints(complexity)
    if type_hints:
        user_parts.append(type_hints)

    user_parts.append(
        f"## Informatica Mapping Data\n"
        f"```\n{chunk.content}\n```"
    )

    if extra_context:
        user_parts.append(f"\n## Additional Context\n{extra_context}")

    user_prompt = "\n\n".join(user_parts)

    logger.debug(
        "Built prompt for '%s' chunk %d/%d: system=%d chars, user=%d chars",
        chunk.mapping_name,
        chunk.chunk_index + 1,
        chunk.total_chunks,
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    return PromptPair(system=SYSTEM_PROMPT, user=user_prompt)


def build_correction_prompt(
    original_files: List["GeneratedFile"],
    validation_errors: List[str],
) -> PromptPair:
    """Build a correction prompt that sends generated code + errors back to the LLM.

    The LLM is asked to fix the specific issues while preserving correct logic.

    Args:
        original_files: The generated files from the previous attempt.
        validation_errors: Human-readable error messages from validation.

    Returns:
        A :class:`PromptPair` for a correction LLM call.
    """
    from informatica_to_dbt.generator.response_parser import GeneratedFile  # noqa: F811

    # Reconstruct the generated output
    file_blocks = []
    for gf in original_files:
        file_blocks.append(f"-- FILE: {gf.path}\n{gf.content}")
    generated_output = "\n\n".join(file_blocks)

    error_list = "\n".join(f"- {e}" for e in validation_errors)

    user_prompt = f"""\
## Correction Request

The previously generated dbt code has validation errors that must be fixed.

### Generated Code (with errors)
```
{generated_output}
```

### Validation Errors Found
{error_list}

### Instructions
1. Fix ALL the listed validation errors.
2. Preserve the overall structure and correct logic — only change what is broken.
3. Key fixes to apply:
   - Replace any remaining Informatica functions (IIF→IFF, ISNULL→IS NULL, \
REPLACESTR→REPLACE, DECODE(TRUE,...)→CASE WHEN, etc.)
   - Remove trailing semicolons from SQL files.
   - Replace hardcoded database.schema.table references with source() or ref().
   - Fix unbalanced parentheses or Jinja braces.
   - Ensure all ref() targets exist as generated model files.
   - Fix YAML parse errors (proper indentation, valid structure).
4. Return the COMPLETE corrected output using the same -- FILE: format.
5. Include ALL files, not just the ones with errors — the output replaces \
the previous attempt entirely.
"""

    logger.debug(
        "Built correction prompt: %d files, %d errors, user=%d chars",
        len(original_files), len(validation_errors), len(user_prompt),
    )

    return PromptPair(system=SYSTEM_PROMPT, user=user_prompt)
