"""Post-process generated dbt files — clean, validate, normalise.

Applies naming conventions, strips trailing whitespace, ensures SQL files
end with a newline, and performs lightweight syntax checks.  Uses the
transformation registry for comprehensive Informatica residual detection.
"""

from __future__ import annotations

import logging
import re
from typing import List, Tuple

from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")


def post_process(files: List[GeneratedFile]) -> Tuple[List[GeneratedFile], List[str]]:
    """Apply post-processing to every generated file.

    Returns:
        A tuple of (processed files, list of warning messages).
    """
    warnings: List[str] = []
    processed: List[GeneratedFile] = []

    for gf in files:
        content = gf.content
        path = gf.path

        # --- Whitespace normalisation ---
        content = _normalise_whitespace(content)

        # --- SQL-specific checks ---
        if gf.is_sql:
            content, sql_warns = _post_process_sql(path, content)
            warnings.extend(sql_warns)

        # --- YAML-specific checks ---
        if gf.is_yaml:
            content, yaml_warns = _post_process_yaml(path, content)
            warnings.extend(yaml_warns)

        # --- Path normalisation ---
        path = _normalise_path(path)

        processed.append(GeneratedFile(path=path, content=content))

    if warnings:
        logger.warning("Post-processing warnings:\n  %s", "\n  ".join(warnings))
    else:
        logger.info("Post-processing completed — no warnings")

    return processed, warnings


# ---------------------------------------------------------------------------
# Whitespace
# ---------------------------------------------------------------------------

def _normalise_whitespace(content: str) -> str:
    """Strip trailing whitespace per line and ensure single trailing newline."""
    lines = [line.rstrip() for line in content.splitlines()]
    # Remove excessive trailing blank lines (keep at most one)
    while len(lines) > 1 and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_CONFIG_RE = re.compile(r"\{\{-?\s*config\s*\(", re.IGNORECASE)
_REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
_SOURCE_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)


def _post_process_sql(path: str, content: str) -> Tuple[str, List[str]]:
    """Validate and clean a SQL model file."""
    warns: List[str] = []

    # Check for config block
    if not _CONFIG_RE.search(content):
        warns.append(f"{path}: Missing {{{{ config(...) }}}} block")

    # Check for hardcoded database/schema names (common LLM mistake)
    hardcoded = _check_hardcoded_references(content)
    if hardcoded:
        warns.append(
            f"{path}: Possible hardcoded database/schema references: "
            f"{', '.join(hardcoded)}"
        )

    # Replace any remaining Informatica expression artefacts
    content = _fix_informatica_residuals(content)

    # --- Snowflake SQL sanitization rules ---
    content = _strip_markdown_fences(content)
    content = _safe_type_conversions(content)
    content = _fix_xmlget_casting(content)
    content = _quote_special_columns(content)

    return content, warns


_HARDCODED_DB_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Z_]\w*\.[A-Z_]\w*\.[A-Z_]\w+)\b",
    re.IGNORECASE,
)


def _check_hardcoded_references(sql: str) -> List[str]:
    """Detect three-part names (DB.SCHEMA.TABLE) that should use ref/source."""
    matches = _HARDCODED_DB_RE.findall(sql)
    # Filter out common false positives (e.g., {{ source(...) }} already there)
    return [m for m in matches if "{{" not in m]


# Comprehensive mapping of Informatica residual patterns to Snowflake SQL.
# Keys are case-insensitive patterns found in generated SQL; values are
# the corrected Snowflake equivalents.
#
# Simple text replacements (function-call form):
_INFORMATICA_FN = {
    # Conditional
    "IIF(":             "IFF(",
    # Null handling — ISNULL(x, default) two-arg form → IFNULL
    # (single-arg boolean form handled by regex below)
    "ISNULL(":          "IFNULL(",
    # NVL is Informatica's null-coalesce — maps to COALESCE in Snowflake
    "NVL(":             "COALESCE(",
    # String — REPLACESTR case_flag stripped via pre-pass regex below
    # "REPLACESTR(":      "REPLACE(",  -- handled by regex to strip case_flag
    "REPLACECHR(":      "TRANSLATE(",
    "REG_EXTRACT(":     "REGEXP_SUBSTR(",
    "REG_REPLACE(":     "REGEXP_REPLACE(",
    "REG_MATCH(":       "REGEXP_LIKE(",
    # Date / Time
    "ADD_TO_DATE(":     "DATEADD(",
    "DATE_DIFF(":       "DATEDIFF(",
    "DATE_COMPARE(":    "DATEDIFF(",
    "GET_DATE_PART(":   "DATE_PART(",
    "SET_DATE_PART(":   "DATE_FROM_PARTS(",
    "SYSTIMESTAMP":     "CURRENT_TIMESTAMP()",
    # Session variables
    "SESSSTARTTIME":    "CURRENT_TIMESTAMP()",
    "SYSDATE":          "CURRENT_TIMESTAMP()",
    # Type conversion
    "TO_INTEGER(":      "TO_NUMBER(",
    "TO_BIGINT(":       "TO_NUMBER(",
    "TO_FLOAT(":        "TO_DOUBLE(",
}

# Patterns that need regex-based replacement (more complex than simple text swap)
_INFORMATICA_REGEX_REPLACEMENTS: List[Tuple[re.Pattern, str, str]] = [
    # :LKP.lookup_name(args) — flag for manual review
    (
        re.compile(r":LKP\.(\w+)\(([^)]*)\)", re.IGNORECASE),
        r"/* TODO: convert :LKP.\1(\2) to LEFT JOIN */",
        ":LKP reference",
    ),
    # ERROR('msg') — typically a default value, not valid SQL
    (
        re.compile(r"\bERROR\s*\(\s*'[^']*'\s*\)", re.IGNORECASE),
        r"NULL /* ERROR default removed — add dbt test */",
        "ERROR() default",
    ),
    # ABORT('msg') — not valid in Snowflake
    (
        re.compile(r"\bABORT\s*\(\s*'[^']*'\s*\)", re.IGNORECASE),
        r"NULL /* ABORT removed — add dbt test */",
        "ABORT() call",
    ),
    # $$PARAMETER references — convert to dbt var
    (
        re.compile(r"\$\$(\w+)"),
        r"{{ var('\1') }}",
        "$$PARAM variable",
    ),
    # $PMmapping.param — convert to dbt var
    (
        re.compile(r"\$PM\w+\.(\w+)"),
        r"{{ var('\1') }}",
        "$PM parameter",
    ),
    # DECODE(TRUE,...) handled by _convert_decode_true() in pre-pass
]


def _split_decode_args(sql: str, start: int) -> Tuple[List[str], int]:
    """Extract comma-separated arguments from a DECODE call, respecting nesting.

    *start* should point to the opening ``(`` after ``DECODE``.
    Returns a tuple of (list-of-argument-strings, end-index-after-closing-paren).
    If the closing paren is not found the original text is left unchanged by
    returning an empty list.
    """
    depth = 0
    args: List[str] = []
    buf: List[str] = []
    i = start
    while i < len(sql):
        ch = sql[i]
        if ch == "(":
            depth += 1
            if depth == 1:
                # skip the opening paren itself
                i += 1
                continue
            buf.append(ch)
        elif ch == ")":
            if depth == 1:
                # closing paren of DECODE
                args.append("".join(buf).strip())
                return args, i + 1
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 1:
            args.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    # Unbalanced — return empty to signal failure
    return [], start


_DECODE_TRUE_RE = re.compile(r"\bDECODE\s*\(\s*TRUE\s*,", re.IGNORECASE)


def _convert_decode_true(sql: str) -> str:
    """Convert ``DECODE(TRUE, cond1, val1, ..., default)`` to CASE WHEN.

    Handles nested function calls in arguments by tracking parenthesis depth.
    Falls back to a TODO comment if parsing fails.
    """
    while True:
        m = _DECODE_TRUE_RE.search(sql)
        if not m:
            break
        # Find the opening paren of DECODE(
        paren_pos = sql.index("(", m.start())
        args, end_pos = _split_decode_args(sql, paren_pos)
        if not args:
            # Parsing failed — leave a TODO marker and break to avoid infinite loop
            sql = sql[:m.start()] + "CASE WHEN /* TODO: DECODE(TRUE,...) parse error */" + sql[m.end():]
            break
        # First arg is TRUE itself (from DECODE(TRUE, ...)).
        # Skip it — the condition/value pairs start at args[1].
        parts = args[1:]
        case_parts: List[str] = []
        i = 0
        while i + 1 < len(parts):
            case_parts.append(f"WHEN {parts[i]} THEN {parts[i + 1]}")
            i += 2
        # Odd trailing argument is the ELSE default
        if i < len(parts):
            case_parts.append(f"ELSE {parts[i]}")
        case_expr = "CASE " + " ".join(case_parts) + " END"
        sql = sql[:m.start()] + case_expr + sql[end_pos:]
    return sql


def _fix_informatica_residuals(sql: str) -> str:
    """Replace leftover Informatica function names and patterns.

    Uses word-boundary matching for function-call patterns to avoid
    corrupting column names (e.g. ``IS_NULL_FLAG`` must not be touched
    when replacing ``ISNULL(``).
    """
    # Pre-pass: convert single-arg ISNULL(expr) boolean form → (expr IS NULL).
    # This must run BEFORE the simple-text loop which converts ISNULL( → IFNULL(.
    # Only matches when there is no comma inside (i.e. single argument).
    sql = re.sub(
        r"\bISNULL\s*\(\s*([^,()]+?)\s*\)",
        r"(\1 IS NULL)",
        sql,
        flags=re.IGNORECASE,
    )

    # Pre-pass: NVL2(expr, not_null_val, null_val) → IFF(expr IS NOT NULL, ..., ...)
    # Must run BEFORE simple-text loop which maps NVL( → COALESCE( (would match NVL2 prefix).
    sql = re.sub(
        r"\bNVL2\s*\(\s*([^,()]+?)\s*,\s*([^,()]+?)\s*,\s*([^,()]+?)\s*\)",
        r"IFF(\1 IS NOT NULL, \2, \3)",
        sql,
        flags=re.IGNORECASE,
    )

    # Pre-pass: REPLACESTR(case_flag, str, old, new) → REPLACE(str, old, new).
    # Informatica's first arg is a numeric case-sensitivity flag; Snowflake's
    # REPLACE doesn't have it, so strip the leading digit argument.
    sql = re.sub(
        r"\bREPLACESTR\s*\(\s*\d+\s*,",
        r"REPLACE(",
        sql,
        flags=re.IGNORECASE,
    )
    # Fallback: REPLACESTR without leading digit (already 3-arg form)
    sql = re.sub(
        r"\bREPLACESTR\s*\(",
        r"REPLACE(",
        sql,
        flags=re.IGNORECASE,
    )

    # Pre-pass: DECODE(TRUE, cond1, val1, ..., default) → CASE WHEN ... END
    sql = _convert_decode_true(sql)

    # Simple text replacements (case-insensitive, word-boundary aware)
    for old, new in _INFORMATICA_FN.items():
        if old.upper() in sql.upper():
            # For function-call patterns like "ISNULL(", match on word boundary
            # before the function name to avoid partial matches inside identifiers.
            fn_name = old.rstrip("(")
            if old.endswith("("):
                pattern = re.compile(r"\b" + re.escape(fn_name) + r"\s*\(", re.IGNORECASE)
                sql = pattern.sub(new, sql)
            else:
                # Non-function tokens (e.g. SYSTIMESTAMP, SYSDATE)
                pattern = re.compile(r"\b" + re.escape(old) + r"\b", re.IGNORECASE)
                sql = pattern.sub(new, sql)

    # Regex-based replacements
    for pattern, replacement, label in _INFORMATICA_REGEX_REPLACEMENTS:
        match = pattern.search(sql)
        if match:
            sql = pattern.sub(replacement, sql)
            logger.debug("Fixed residual %s in generated SQL", label)

    return sql


# ---------------------------------------------------------------------------
# Snowflake SQL sanitization helpers
# ---------------------------------------------------------------------------

_MARKDOWN_FENCE_RE = re.compile(r"^```(?:sql)?\s*$", re.MULTILINE)


def _strip_markdown_fences(sql: str) -> str:
    """Remove markdown code fences that LLMs sometimes wrap around SQL."""
    return _MARKDOWN_FENCE_RE.sub("", sql).strip() + "\n"


# TO_NUMBER / TO_DATE → TRY_TO_NUMBER / TRY_TO_DATE  (avoid runtime errors on bad data)
_TO_NUMBER_RE = re.compile(r"\bTO_NUMBER\s*\(", re.IGNORECASE)
_TO_DATE_RE = re.compile(r"\bTO_DATE\s*\(", re.IGNORECASE)
_TO_TIMESTAMP_RE = re.compile(r"\bTO_TIMESTAMP\s*\(", re.IGNORECASE)


def _safe_type_conversions(sql: str) -> str:
    """Replace strict type conversions with TRY_ variants for resilience."""
    # Don't replace if already TRY_ variant
    sql = re.sub(r"(?<!\w)(?<!TRY_)TO_NUMBER\s*\(", "TRY_TO_NUMBER(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(?<!\w)(?<!TRY_)TO_DATE\s*\(", "TRY_TO_DATE(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(?<!\w)(?<!TRY_)TO_TIMESTAMP\s*\(", "TRY_TO_TIMESTAMP(", sql, flags=re.IGNORECASE)
    return sql


# XMLGET returns OBJECT type — must use :"$" to extract text before casting
_XMLGET_CAST_RE = re.compile(
    r"(XMLGET\s*\([^)]+\))\s*::\s*(STRING|VARCHAR|NUMBER|INT|INTEGER|FLOAT|BOOLEAN|DATE|TIMESTAMP)",
    re.IGNORECASE,
)


def _fix_xmlget_casting(sql: str) -> str:
    """Fix XMLGET(...)::TYPE → XMLGET(...):"$"::TYPE for Snowflake OBJECT extraction."""
    def _replace_xmlget(m: re.Match) -> str:
        xmlget_expr = m.group(1)
        cast_type = m.group(2).upper()
        # Already has :"$" — skip
        if ':"$"' in xmlget_expr:
            return m.group(0)
        if cast_type == "BOOLEAN":
            return f'TRY_TO_BOOLEAN({xmlget_expr}:"$"::STRING)'
        return f'{xmlget_expr}:"$"::{cast_type}'
    return _XMLGET_CAST_RE.sub(_replace_xmlget, sql)


# Columns with special characters (#, /) need double-quoting in Snowflake
_SPECIAL_COL_RE = re.compile(
    r'(?<!")(\b[A-Z_]\w*(?:[#/][A-Z_]\w*)+)\b(?!")',
    re.IGNORECASE,
)


def _quote_special_columns(sql: str) -> str:
    """Double-quote column names containing # or / characters."""
    def _quote_match(m: re.Match) -> str:
        col = m.group(1)
        # Skip if inside a Jinja expression or string literal
        start = m.start()
        prefix = sql[max(0, start - 5):start]
        if "'" in prefix or "{" in prefix:
            return col
        return f'"{col}"'
    return _SPECIAL_COL_RE.sub(_quote_match, sql)


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------

def _post_process_yaml(path: str, content: str) -> Tuple[str, List[str]]:
    """Validate and clean a YAML file."""
    warns: List[str] = []

    if "version:" not in content:
        warns.append(f"{path}: Missing 'version:' key in YAML")

    # Ensure no tab characters (YAML must use spaces)
    if "\t" in content:
        warns.append(f"{path}: Tabs found in YAML — replacing with spaces")
        content = content.replace("\t", "  ")

    return content, warns


# ---------------------------------------------------------------------------
# Path normalisation
# ---------------------------------------------------------------------------

def _normalise_path(path: str) -> str:
    """Ensure path is relative and uses forward slashes."""
    path = path.replace("\\", "/")
    # Strip leading ./ or /
    path = re.sub(r"^\.?/", "", path)
    return path
