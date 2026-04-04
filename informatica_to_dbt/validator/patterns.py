"""Shared regex patterns for SQL validation and quality scoring.

These patterns detect Informatica residuals, dbt conventions, and common
SQL issues.  Used by both ``sql_validator`` and ``quality_scorer`` to
avoid duplication.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Informatica residual detection
# ---------------------------------------------------------------------------

INFORMATICA_FN_RE = re.compile(
    r"\b("
    r"IIF\s*\(|ISNULL\s*\(|REPLACESTR\s*\(|REPLACECHR\s*\(|"
    r"REG_EXTRACT\s*\(|REG_REPLACE\s*\(|REG_MATCH\s*\(|"
    r"ADD_TO_DATE\s*\(|DATE_DIFF\s*\(|DATE_COMPARE\s*\(|"
    r"GET_DATE_PART\s*\(|SET_DATE_PART\s*\(|"
    r"TO_INTEGER\s*\(|TO_BIGINT\s*\(|TO_FLOAT\s*\(|"
    r"SESSSTARTTIME|SYSTIMESTAMP|DECODE\s*\(\s*TRUE|"
    r"NVL2?\s*\(|\bSYSDATE\b|\bDECODE\s*\("
    r")",
    re.IGNORECASE,
)

INFORMATICA_PATTERN_RE = re.compile(
    r"("
    r":LKP\.\w+\s*\(|"
    r"\$\$\w+|"
    r"\$PM\w+\.\w+|"
    r"\bABORT\s*\(\s*'|"
    r"\bERROR\s*\(\s*'"
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# dbt convention patterns
# ---------------------------------------------------------------------------

CONFIG_RE = re.compile(r"\{\{-?\s*config\s*\(", re.IGNORECASE)
REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
SOURCE_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)

# ---------------------------------------------------------------------------
# SQL syntax patterns
# ---------------------------------------------------------------------------

SEMICOLON_RE = re.compile(r";\s*$", re.MULTILINE)
