"""Data-driven registry of Informatica PowerCenter transformation types.

Provides a single source of truth for:
- All known Informatica transformation types (30+)
- Complexity weight per type (used by the complexity analyzer)
- dbt / Snowflake SQL pattern each type maps to
- Context-criticality flags (used by the chunker to decide what to keep)
- Informatica → Snowflake function equivalents

The registry is intentionally comprehensive.  Unknown types encountered at
runtime are handled gracefully with sensible defaults so the framework
never hard-fails on a new transformation type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ContextPriority(Enum):
    """How important it is to keep this transformation's full detail in a
    chunk sent to the LLM."""

    CRITICAL = "critical"      # Must always be included in full
    HIGH = "high"              # Include if space allows
    MEDIUM = "medium"          # Summarise if space is tight
    LOW = "low"                # Can be omitted / referenced by name only


class DbtPattern(Enum):
    """Broad dbt / SQL pattern the transformation maps to."""

    PASSTHROUGH = "passthrough"            # SELECT col AS col
    COLUMN_EXPRESSION = "column_expression"  # SELECT expr AS col
    JOIN = "join"                          # JOIN ... ON ...
    LEFT_JOIN = "left_join"                # LEFT JOIN (lookups)
    FILTER = "filter"                      # WHERE ...
    CASE_WHEN = "case_when"                # CASE WHEN (router groups)
    GROUP_BY = "group_by"                  # GROUP BY + aggregates
    WINDOW_FUNCTION = "window_function"    # ROW_NUMBER / RANK / DENSE_RANK
    ORDER_BY = "order_by"                  # ORDER BY (sorter)
    UNPIVOT = "unpivot"                    # UNPIVOT (normalizer)
    UNION = "union"                        # UNION ALL
    MACRO = "macro"                        # dbt macro / reusable SQL
    CTE = "cte"                            # WITH ... AS (...)
    INCREMENTAL = "incremental"            # dbt incremental strategy
    UPDATE_STRATEGY = "update_strategy"    # MERGE / incremental logic
    SEQUENCE = "sequence"                  # ROW_NUMBER() or IDENTITY
    STORED_PROCEDURE = "stored_procedure"  # Snowflake stored proc / macro
    XML_PARSE = "xml_parse"                # PARSE_XML / XMLGET
    CUSTOM = "custom"                      # Needs manual mapping


# ---------------------------------------------------------------------------
# Transformation type descriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TransformationType:
    """Descriptor for a single Informatica transformation type."""

    name: str
    complexity_weight: float              # 0.0–1.0 contribution to score
    dbt_pattern: DbtPattern
    context_priority: ContextPriority
    description: str = ""
    # Which TABLEATTRIBUTE names carry critical logic for this type
    critical_attributes: Tuple[str, ...] = ()
    # Short conversion hint included in LLM prompts
    conversion_hint: str = ""


# ---------------------------------------------------------------------------
# The Registry
# ---------------------------------------------------------------------------

_TRANSFORMATION_TYPES: Dict[str, TransformationType] = {}


def _register(t: TransformationType) -> None:
    _TRANSFORMATION_TYPES[t.name] = t


# --- Source Qualifiers ---
_register(TransformationType(
    name="Source Qualifier",
    complexity_weight=0.1,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.HIGH,
    description="Standard relational source reader with optional SQL override.",
    critical_attributes=("Sql Query", "Source Filter", "User Defined Join",
                         "Number Of Sorted Ports", "Select Distinct"),
    conversion_hint="If 'Sql Query' override exists, use it as the base SELECT. "
                    "Otherwise generate SELECT from source columns. "
                    "Apply 'Source Filter' as WHERE clause.",
))

_register(TransformationType(
    name="App Multi-Group Source Qualifier",
    complexity_weight=0.15,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.HIGH,
    description="Application-specific source qualifier (SAP BW OHS, etc.).",
    critical_attributes=("Sql Query", "Source Filter"),
    conversion_hint="Treat identically to Source Qualifier. "
                    "Map to a Snowflake source() reference.",
))

_register(TransformationType(
    name="XML Source Qualifier",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.XML_PARSE,
    context_priority=ContextPriority.HIGH,
    description="XML file source reader with XPath-based extraction.",
    critical_attributes=("Xml Root Tag", "Xml Row Tag"),
    conversion_hint="Use Snowflake PARSE_XML / XMLGET or FLATTEN on "
                    "semi-structured data. Reference source() for raw XML stage.",
))

# --- Expression-Based ---
_register(TransformationType(
    name="Expression",
    complexity_weight=0.15,
    dbt_pattern=DbtPattern.COLUMN_EXPRESSION,
    context_priority=ContextPriority.CRITICAL,
    description="Column-level transformations via Informatica expression language.",
    critical_attributes=(),
    conversion_hint="Convert each OUTPUT port expression to a Snowflake SQL "
                    "expression. Map Informatica functions (IIF→IFF, "
                    "ISNULL→IS NULL, DECODE→CASE, etc.).",
))

_register(TransformationType(
    name="Expression Macro",
    complexity_weight=0.1,
    dbt_pattern=DbtPattern.MACRO,
    context_priority=ContextPriority.HIGH,
    description="Reusable expression macro (EXPRMACRO).",
    critical_attributes=(),
    conversion_hint="Convert to a dbt macro. Replace REG_REPLACE/REG_EXTRACT "
                    "with Snowflake REGEXP_REPLACE/REGEXP_SUBSTR.",
))

# --- Lookup ---
_register(TransformationType(
    name="Lookup Procedure",
    complexity_weight=0.25,
    dbt_pattern=DbtPattern.LEFT_JOIN,
    context_priority=ContextPriority.CRITICAL,
    description="Lookup against a database table or flat file.",
    critical_attributes=("Lookup Sql Override", "Lookup condition",
                         "Lookup table name", "Connection Information",
                         "Lookup Policy on Multiple Match"),
    conversion_hint="Convert to LEFT JOIN on the lookup condition. "
                    "If 'Lookup Sql Override' exists, use it as a CTE. "
                    "Respect 'Lookup Policy on Multiple Match' — "
                    "use QUALIFY ROW_NUMBER()=1 if needed.",
))

# --- Routing / Filtering ---
_register(TransformationType(
    name="Router",
    complexity_weight=0.3,
    dbt_pattern=DbtPattern.CASE_WHEN,
    context_priority=ContextPriority.CRITICAL,
    description="Splits rows into groups based on conditions.",
    critical_attributes=(),
    conversion_hint="Each Router GROUP becomes a CASE WHEN branch or "
                    "a separate CTE with its group filter condition. "
                    "The DEFAULT group is the ELSE / NOT matched rows.",
))

_register(TransformationType(
    name="Filter",
    complexity_weight=0.15,
    dbt_pattern=DbtPattern.FILTER,
    context_priority=ContextPriority.CRITICAL,
    description="Filters rows based on a condition expression.",
    critical_attributes=("Filter Condition",),
    conversion_hint="Convert Filter Condition to a WHERE clause. "
                    "Translate Informatica functions to Snowflake equivalents.",
))

# --- Joiner / Sorter ---
_register(TransformationType(
    name="Joiner",
    complexity_weight=0.25,
    dbt_pattern=DbtPattern.JOIN,
    context_priority=ContextPriority.CRITICAL,
    description="Joins two data streams (Master/Detail).",
    critical_attributes=("Join Condition", "Join Type",
                         "Sorted Input"),
    conversion_hint="Map 'Join Type' to SQL JOIN type: "
                    "Normal→INNER JOIN, Master Outer→LEFT JOIN, "
                    "Detail Outer→RIGHT JOIN, Full Outer→FULL OUTER JOIN. "
                    "Convert the Join Condition to ON clause.",
))

_register(TransformationType(
    name="Sorter",
    complexity_weight=0.1,
    dbt_pattern=DbtPattern.ORDER_BY,
    context_priority=ContextPriority.MEDIUM,
    description="Sorts data by specified key ports.",
    critical_attributes=("Sorter Condition", "Distinct",
                         "Case Sensitive", "Null Treated Low"),
    conversion_hint="In dbt/Snowflake, sorting is generally implicit. "
                    "Add ORDER BY in window functions if downstream logic "
                    "depends on order. If DISTINCT=YES, add SELECT DISTINCT.",
))

# --- Aggregator ---
_register(TransformationType(
    name="Aggregator",
    complexity_weight=0.25,
    dbt_pattern=DbtPattern.GROUP_BY,
    context_priority=ContextPriority.CRITICAL,
    description="Groups rows and applies aggregate functions (SUM, COUNT, etc.).",
    critical_attributes=("Sorted Input", "Cache Directory"),
    conversion_hint="Group-by ports become the GROUP BY columns. "
                    "Output ports with aggregate expressions map to "
                    "SUM/COUNT/MIN/MAX/AVG. Nested IIF → IFF in aggregates.",
))

# --- Rank ---
_register(TransformationType(
    name="Rank",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.WINDOW_FUNCTION,
    context_priority=ContextPriority.HIGH,
    description="Returns top/bottom N rows per group.",
    critical_attributes=("Top/Bottom", "Number of Ranks",
                         "Case Sensitive"),
    conversion_hint="Convert to ROW_NUMBER() OVER (PARTITION BY group_port "
                    "ORDER BY rank_port ASC/DESC) and filter ≤ N.",
))

# --- Normalizer ---
_register(TransformationType(
    name="Normalizer",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.UNPIVOT,
    context_priority=ContextPriority.HIGH,
    description="Converts a single row into multiple rows (one-to-many).",
    critical_attributes=(),
    conversion_hint="Convert to Snowflake UNPIVOT or LATERAL FLATTEN. "
                    "The GENERATED_COLUMN_ID maps to a generated index.",
))

# --- Update Strategy ---
_register(TransformationType(
    name="Update Strategy",
    complexity_weight=0.3,
    dbt_pattern=DbtPattern.UPDATE_STRATEGY,
    context_priority=ContextPriority.CRITICAL,
    description="Flags rows for INSERT, UPDATE, DELETE, or REJECT.",
    critical_attributes=("Update Strategy Expression",),
    conversion_hint="DD_INSERT→INSERT, DD_UPDATE→UPDATE, DD_DELETE→DELETE, "
                    "DD_REJECT→filtered out. In dbt: use incremental model "
                    "with merge strategy. Map the expression to "
                    "is_incremental() logic.",
))

# --- Sequence Generator ---
_register(TransformationType(
    name="Sequence Generator",
    complexity_weight=0.05,
    dbt_pattern=DbtPattern.SEQUENCE,
    context_priority=ContextPriority.LOW,
    description="Generates sequential numeric IDs.",
    critical_attributes=("Start Value", "Increment By", "End Value",
                         "Current Value", "Cycle", "Reset"),
    conversion_hint="Use ROW_NUMBER() OVER (ORDER BY ...) or a Snowflake "
                    "SEQUENCE object. For surrogate keys in dbt, prefer "
                    "dbt_utils.generate_surrogate_key().",
))

# --- Mapplet ---
_register(TransformationType(
    name="Mapplet",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.MACRO,
    context_priority=ContextPriority.HIGH,
    description="Reusable transformation logic packaged as a mapplet.",
    critical_attributes=(),
    conversion_hint="Convert to a dbt macro or a CTE that encapsulates "
                    "the mapplet's internal transformations. Include the "
                    "Input/Output Transformation port mappings.",
))

_register(TransformationType(
    name="Input Transformation",
    complexity_weight=0.05,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.MEDIUM,
    description="Input interface of a Mapplet.",
    critical_attributes=(),
    conversion_hint="Maps to macro input parameters or CTE column references.",
))

_register(TransformationType(
    name="Output Transformation",
    complexity_weight=0.05,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.MEDIUM,
    description="Output interface of a Mapplet.",
    critical_attributes=(),
    conversion_hint="Maps to macro return columns or final CTE SELECT.",
))

# --- Union ---
_register(TransformationType(
    name="Union",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.UNION,
    context_priority=ContextPriority.HIGH,
    description="Merges multiple input groups into one output.",
    critical_attributes=(),
    conversion_hint="Convert to UNION ALL of the input groups. "
                    "Ensure column alignment between groups.",
))

_register(TransformationType(
    name="Custom Transformation",
    complexity_weight=0.35,
    dbt_pattern=DbtPattern.CUSTOM,
    context_priority=ContextPriority.CRITICAL,
    description="Custom / Java transformation — requires manual inspection.",
    critical_attributes=(),
    conversion_hint="Inspect the transformation logic manually. May need "
                    "a Snowflake UDF or stored procedure equivalent.",
))

# --- Stored Procedure ---
_register(TransformationType(
    name="Stored Procedure",
    complexity_weight=0.35,
    dbt_pattern=DbtPattern.STORED_PROCEDURE,
    context_priority=ContextPriority.CRITICAL,
    description="Calls an external stored procedure.",
    critical_attributes=("Stored Procedure Name", "Connection Information"),
    conversion_hint="Convert to a dbt run-operation macro calling a "
                    "Snowflake stored procedure, or inline the logic.",
))

# --- Transaction Control ---
_register(TransformationType(
    name="Transaction Control",
    complexity_weight=0.15,
    dbt_pattern=DbtPattern.CUSTOM,
    context_priority=ContextPriority.HIGH,
    description="Controls commit/rollback boundaries.",
    critical_attributes=("Transaction Control Condition",),
    conversion_hint="Snowflake auto-commits per statement in dbt. "
                    "Document the original transaction boundaries as "
                    "comments. If critical, use a post-hook.",
))

# --- SQL Transformation ---
_register(TransformationType(
    name="SQL Transformation",
    complexity_weight=0.3,
    dbt_pattern=DbtPattern.CTE,
    context_priority=ContextPriority.CRITICAL,
    description="Executes inline SQL within the mapping.",
    critical_attributes=("Sql Query",),
    conversion_hint="Embed the SQL directly as a CTE. "
                    "Translate Oracle/SQL Server dialect to Snowflake SQL.",
))

# --- Java Transformation ---
_register(TransformationType(
    name="Java Transformation",
    complexity_weight=0.4,
    dbt_pattern=DbtPattern.CUSTOM,
    context_priority=ContextPriority.CRITICAL,
    description="Java code transformation — requires manual conversion.",
    critical_attributes=(),
    conversion_hint="Requires manual conversion. Consider a Snowflake "
                    "Java UDF or rewrite in SQL if logic is simple.",
))

# --- HTTP Transformation ---
_register(TransformationType(
    name="HTTP Transformation",
    complexity_weight=0.3,
    dbt_pattern=DbtPattern.CUSTOM,
    context_priority=ContextPriority.CRITICAL,
    description="Calls an HTTP endpoint during data flow.",
    critical_attributes=("Base URL", "HTTP Method"),
    conversion_hint="Convert to an External Function or "
                    "Snowflake External Network Access + UDF.",
))

# --- Data Masking ---
_register(TransformationType(
    name="Data Masking",
    complexity_weight=0.15,
    dbt_pattern=DbtPattern.COLUMN_EXPRESSION,
    context_priority=ContextPriority.HIGH,
    description="Masks sensitive data fields.",
    critical_attributes=(),
    conversion_hint="Use Snowflake Dynamic Data Masking policies or "
                    "SHA2/MD5 hash functions in the SELECT.",
))

# --- Unconnected Lookup ---
_register(TransformationType(
    name="Unconnected Lookup",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.LEFT_JOIN,
    context_priority=ContextPriority.HIGH,
    description="Lookup called from an expression via :LKP.LOOKUP_NAME().",
    critical_attributes=("Lookup Sql Override", "Lookup condition"),
    conversion_hint="Convert :LKP.name(port) calls to a LEFT JOIN or "
                    "correlated subquery on the lookup table.",
))

# --- External Procedure ---
_register(TransformationType(
    name="External Procedure",
    complexity_weight=0.3,
    dbt_pattern=DbtPattern.STORED_PROCEDURE,
    context_priority=ContextPriority.CRITICAL,
    description="Calls an external procedure or COM object.",
    critical_attributes=("Procedure Name",),
    conversion_hint="Requires manual conversion. Map to a Snowflake "
                    "stored procedure or external function.",
))

# --- Source / Target (treated as pseudo-transformations in some contexts) ---
_register(TransformationType(
    name="Source Definition",
    complexity_weight=0.05,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.HIGH,
    description="Source table definition (not a transformation per se).",
    critical_attributes=(),
    conversion_hint="Map to a dbt source() reference in sources.yml.",
))

_register(TransformationType(
    name="Target Definition",
    complexity_weight=0.05,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.HIGH,
    description="Target table definition.",
    critical_attributes=(),
    conversion_hint="The target table becomes the dbt model name.",
))

# --- Midstream Passive ---
_register(TransformationType(
    name="Midstream",
    complexity_weight=0.1,
    dbt_pattern=DbtPattern.PASSTHROUGH,
    context_priority=ContextPriority.LOW,
    description="Passive midstream transformation for data lineage.",
    critical_attributes=(),
    conversion_hint="Typically passthrough — omit from generated SQL.",
))


# ---------------------------------------------------------------------------
# Informatica → Snowflake function mapping
# ---------------------------------------------------------------------------

# Each entry: Informatica pattern → (Snowflake equivalent, notes)
FUNCTION_MAP: Dict[str, Tuple[str, str]] = {
    # Conditional
    "IIF":              ("IFF", "IIF(cond, true, false) → IFF(cond, true, false)"),
    "DECODE":           ("CASE/DECODE", "DECODE(TRUE, c1, v1, ...) → CASE WHEN c1 THEN v1 ... END. "
                                        "Simple DECODE(col, v1, r1, ...) → Snowflake DECODE()."),
    # Null handling
    "ISNULL":           ("IS NULL", "ISNULL(x) → x IS NULL (boolean context) or COALESCE(x, default)"),
    "NVL":              ("NVL / COALESCE", "NVL(a, b) → NVL(a, b) or COALESCE(a, b)"),
    "NVL2":             ("NVL2", "NVL2(expr, not_null_val, null_val) → NVL2(expr, v1, v2)"),
    # String
    "LTRIM":            ("LTRIM", "Direct equivalent."),
    "RTRIM":            ("RTRIM", "Direct equivalent."),
    "SUBSTR":           ("SUBSTR", "SUBSTR(str, start, len) — same in Snowflake."),
    "LPAD":             ("LPAD", "Direct equivalent."),
    "RPAD":             ("RPAD", "Direct equivalent."),
    "UPPER":            ("UPPER", "Direct equivalent."),
    "LOWER":            ("LOWER", "Direct equivalent."),
    "INITCAP":          ("INITCAP", "Direct equivalent."),
    "LENGTH":           ("LENGTH", "Direct equivalent."),
    "INSTR":            ("POSITION / CHARINDEX", "INSTR(str, sub) → POSITION(sub IN str) or CHARINDEX(sub, str)."),
    "REPLACECHR":       ("TRANSLATE / REPLACE",
                         "REPLACECHR(0, str, chars, repl) → TRANSLATE(str, chars, repl). "
                         "If repl is NULL → REPLACE each char individually."),
    "REPLACESTR":       ("REPLACE", "REPLACESTR(1, str, old, new) → REPLACE(str, old, new)."),
    "REG_EXTRACT":      ("REGEXP_SUBSTR", "REG_EXTRACT(str, pattern, group) → REGEXP_SUBSTR(str, pattern, 1, 1, 'e', group)."),
    "REG_REPLACE":      ("REGEXP_REPLACE", "REG_REPLACE(str, pattern, repl) → REGEXP_REPLACE(str, pattern, repl)."),
    "REG_MATCH":        ("REGEXP_LIKE / RLIKE", "REG_MATCH(str, pattern) → REGEXP_LIKE(str, pattern)."),
    "CHR":              ("CHR", "CHR(n) — direct equivalent. CHR(34) = double-quote."),
    "ASCII":            ("ASCII", "Direct equivalent."),
    "CONCAT":           ("CONCAT / ||", "CONCAT(a, b) or a || b."),
    "REVERSE":          ("REVERSE", "Direct equivalent."),
    # Numeric / Math
    "TRUNC":            ("TRUNC", "TRUNC(num, scale) — direct equivalent."),
    "ROUND":            ("ROUND", "Direct equivalent."),
    "MOD":              ("MOD", "Direct equivalent."),
    "ABS":              ("ABS", "Direct equivalent."),
    "CEIL":             ("CEIL", "Direct equivalent."),
    "FLOOR":            ("FLOOR", "Direct equivalent."),
    "POWER":            ("POWER / POW", "Direct equivalent."),
    "SQRT":             ("SQRT", "Direct equivalent."),
    "LOG":              ("LOG / LN", "LOG(base, val) or LN(val)."),
    "SIGN":             ("SIGN", "Direct equivalent."),
    # Type conversion
    "TO_CHAR":          ("TO_CHAR / TO_VARCHAR",
                         "TO_CHAR(date, 'YYYYMMDD') → TO_CHAR(date, 'YYYYMMDD'). "
                         "TO_CHAR(num) → TO_VARCHAR(num)."),
    "TO_DATE":          ("TO_DATE / TRY_TO_DATE", "TO_DATE(str, fmt) → TRY_TO_DATE(str, fmt)."),
    "TO_DECIMAL":       ("TO_DECIMAL / TO_NUMBER",
                         "TO_DECIMAL(str, scale) → TO_DECIMAL(str, 38, scale). "
                         "Or TO_NUMBER(str, fmt)."),
    "TO_INTEGER":       ("TO_NUMBER", "TO_INTEGER(x) → TO_NUMBER(x, 38, 0)."),
    "TO_BIGINT":        ("TO_NUMBER", "TO_BIGINT(x) → TO_NUMBER(x, 38, 0)."),
    "TO_FLOAT":         ("TO_DOUBLE", "TO_FLOAT(x) → TO_DOUBLE(x)."),
    # Date / Time
    "SYSDATE":          ("CURRENT_TIMESTAMP()", "System variable → CURRENT_TIMESTAMP()."),
    "SESSSTARTTIME":    ("CURRENT_TIMESTAMP()", "Session variable → CURRENT_TIMESTAMP()."),
    "SYSTIMESTAMP":     ("CURRENT_TIMESTAMP()", "High-precision timestamp → CURRENT_TIMESTAMP()."),
    "GET_DATE_PART":    ("DATE_PART", "GET_DATE_PART(part, date) → DATE_PART(part, date)."),
    "DATE_DIFF":        ("DATEDIFF", "DATE_DIFF(d1, d2, 'D') → DATEDIFF('DAY', d2, d1)."),
    "DATE_COMPARE":     ("DATEDIFF / comparison", "DATE_COMPARE(d1, d2) → SIGN(DATEDIFF('DAY', d2, d1))."),
    "ADD_TO_DATE":      ("DATEADD", "ADD_TO_DATE(date, 'MM', n) → DATEADD('MONTH', n, date)."),
    "LAST_DAY":         ("LAST_DAY", "Direct equivalent."),
    "SET_DATE_PART":    ("DATE_FROM_PARTS + DATE_PART",
                         "No direct equivalent — reconstruct with DATE_FROM_PARTS."),
    "TRUNC (date)":     ("DATE_TRUNC", "TRUNC(date, 'MM') → DATE_TRUNC('MONTH', date)."),
    # Aggregate (used in Aggregator transformations)
    "SUM":              ("SUM", "Direct equivalent."),
    "COUNT":            ("COUNT", "Direct equivalent."),
    "MIN":              ("MIN", "Direct equivalent."),
    "MAX":              ("MAX", "Direct equivalent."),
    "AVG":              ("AVG", "Direct equivalent."),
    "MEDIAN":           ("MEDIAN", "Direct equivalent."),
    "FIRST":            ("FIRST_VALUE", "FIRST(col) → FIRST_VALUE(col) in window or MIN."),
    "LAST":             ("LAST_VALUE", "LAST(col) → LAST_VALUE(col) in window or MAX."),
    # Hash
    "MD5":              ("MD5", "MD5(str) — direct equivalent."),
    "CRC32":            ("CRC32", "Not natively in Snowflake — use MD5 or SHA2 instead."),
    # Lookup reference
    ":LKP":             ("LEFT JOIN / subquery",
                         ":LKP.LOOKUP_NAME(port) → LEFT JOIN on the lookup table "
                         "or a correlated subquery."),
    # Special
    "ABORT":            ("-- ABORT (commented)",
                         "ABORT('msg') has no dbt equivalent. "
                         "Convert to a dbt test or macro assertion."),
    "ERROR":            ("-- ERROR (commented)",
                         "ERROR('msg') as DEFAULTVALUE — typically dead-letter logic. "
                         "Convert to a dbt test or omit default."),
    # Variables / Parameters
    "$$":               ("var() / env_var()",
                         "$$PARAM_NAME → dbt var('param_name') or env_var('PARAM_NAME')."),
    "$PM":              ("var()",
                         "$PMmapping_name.param → dbt var('param'). "
                         "Session parameters mapped to dbt project vars."),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Default for unknown transformation types
_DEFAULT_TYPE = TransformationType(
    name="Unknown",
    complexity_weight=0.2,
    dbt_pattern=DbtPattern.CUSTOM,
    context_priority=ContextPriority.HIGH,
    description="Unrecognized transformation type — needs manual review.",
    critical_attributes=(),
    conversion_hint="Unknown transformation type. Inspect the XML and "
                    "convert manually based on port expressions.",
)


def get_transformation_type(name: str) -> TransformationType:
    """Look up a transformation type by its Informatica TYPE string.

    Returns the default descriptor if *name* is not in the registry, so
    callers never need to handle ``None``.
    """
    return _TRANSFORMATION_TYPES.get(name, _DEFAULT_TYPE)


def get_all_types() -> Dict[str, TransformationType]:
    """Return a copy of the full transformation type registry."""
    return dict(_TRANSFORMATION_TYPES)


def get_critical_attributes_for_type(type_name: str) -> Tuple[str, ...]:
    """Return the TABLEATTRIBUTE names that carry critical logic for *type_name*."""
    return get_transformation_type(type_name).critical_attributes


def get_conversion_hint(type_name: str) -> str:
    """Return the short conversion hint for a given transformation type."""
    return get_transformation_type(type_name).conversion_hint


def get_function_equivalent(infa_fn: str) -> Optional[Tuple[str, str]]:
    """Look up the Snowflake equivalent for an Informatica function name.

    Returns ``(snowflake_fn, notes)`` or ``None`` if not mapped.
    """
    return FUNCTION_MAP.get(infa_fn)


def get_all_function_names() -> List[str]:
    """Return all known Informatica function names (for regex building)."""
    return list(FUNCTION_MAP.keys())


def get_context_priority(type_name: str) -> ContextPriority:
    """Return the chunking context-priority for a transformation type."""
    return get_transformation_type(type_name).context_priority


def get_dbt_pattern(type_name: str) -> DbtPattern:
    """Return the target dbt/SQL pattern for a transformation type."""
    return get_transformation_type(type_name).dbt_pattern


def complexity_weight(type_name: str) -> float:
    """Return the complexity weight for a transformation type."""
    return get_transformation_type(type_name).complexity_weight


def build_function_regex_pattern() -> str:
    """Build a regex alternation pattern matching all Informatica function names.

    Suitable for use in ``re.compile()`` for residual-detection scanning.
    Escapes special regex characters in function names (e.g. ``$$``).
    """
    import re
    names = []
    for fn in FUNCTION_MAP:
        # Skip patterns that are prefixes/variables rather than function calls
        if fn.startswith("$"):
            names.append(re.escape(fn))
        elif fn == ":LKP":
            names.append(r":LKP\b")
        else:
            names.append(re.escape(fn) + r"\s*\(")
    return "|".join(names)
