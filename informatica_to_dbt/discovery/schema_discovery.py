"""Discover source table schemas from Snowflake or Informatica XML.

Three modes:

1. **Snowflake mode**: Query ``INFORMATION_SCHEMA.COLUMNS`` to build a
   table→column map for source definitions.
2. **XML mode**: Extract source/target tables and columns directly from
   the parsed Informatica XML (no Snowflake connection needed).
3. **Manual mode**: Read from a user-provided ``source_map.json``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import re

logger = logging.getLogger("informatica_dbt")


def _sanitize_identifier(name: str) -> str:
    """Strip characters that are not valid in Snowflake identifiers.

    Prevents SQL injection by allowing only alphanumeric, underscore,
    and dollar-sign characters.  Raises ValueError on empty result.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_$]", "", name)
    if not cleaned:
        raise ValueError(f"Invalid Snowflake identifier: {name!r}")
    return cleaned


@dataclass
class TableSchema:
    """Schema information for a single table."""

    database: str = ""
    schema: str = ""
    table_name: str = ""
    columns: List[Dict[str, str]] = field(default_factory=list)
    row_count: Optional[int] = None


@dataclass
class SchemaDiscovery:
    """Manages source table schema information.

    Collects table metadata from one or more sources (Snowflake, XML, JSON)
    and provides a unified lookup interface.
    """

    tables: Dict[str, TableSchema] = field(default_factory=dict)

    def get(self, table_name: str) -> Optional[TableSchema]:
        """Look up a table schema by name (case-insensitive)."""
        return self.tables.get(table_name.upper())

    def add(self, schema: TableSchema) -> None:
        """Register a table schema."""
        key = schema.table_name.upper()
        self.tables[key] = schema

    @property
    def table_count(self) -> int:
        return len(self.tables)

    def to_source_map(self) -> Dict[str, Dict[str, Any]]:
        """Export as a source_map-compatible dictionary."""
        result: Dict[str, Dict[str, Any]] = {}
        for key, ts in self.tables.items():
            result[key] = {
                "database": ts.database,
                "schema": ts.schema,
                "columns": ts.columns,
            }
        return result

    def save_json(self, path: str) -> None:
        """Write the source map to a JSON file."""
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(self.to_source_map(), indent=2), encoding="utf-8"
        )
        logger.info("Wrote source map with %d tables to %s", self.table_count, path)

    # ------------------------------------------------------------------
    # From Snowflake
    # ------------------------------------------------------------------

    @classmethod
    def from_snowflake(
        cls,
        session: Any,
        database: str,
        schema: str,
    ) -> SchemaDiscovery:
        """Discover table schemas from Snowflake INFORMATION_SCHEMA.

        Args:
            session: A Snowpark ``Session`` object.
            database: Snowflake database name.
            schema: Snowflake schema name.

        Returns:
            A populated :class:`SchemaDiscovery` instance.
        """
        discovery = cls()

        safe_db = _sanitize_identifier(database)
        safe_schema = _sanitize_identifier(schema)

        # Use a parameterized query for the WHERE clause value to avoid
        # SQL injection via string interpolation.  The database/schema
        # identifiers in the FROM clause cannot use bind parameters, but
        # _sanitize_identifier() strips everything except [A-Za-z0-9_$]
        # so they are safe for interpolation (M2 fix).
        query = (
            f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
            f"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
            f"FROM {safe_db}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = ? "
            f"ORDER BY TABLE_NAME, ORDINAL_POSITION"
        )

        try:
            cursor = session.connection.cursor()
            try:
                cursor.execute(query, (safe_schema,))
                rows = cursor.fetchall()
                # Map positional results to dict-like access expected below
                col_names = [desc[0] for desc in cursor.description]
                rows = [dict(zip(col_names, row)) for row in rows]
            finally:
                cursor.close()
        except Exception as exc:
            logger.error("Schema discovery query failed: %s", exc)
            return discovery

        current_table: Optional[str] = None
        current_cols: List[Dict[str, str]] = []

        for row in rows:
            tbl = row["TABLE_NAME"]
            if tbl != current_table:
                if current_table is not None:
                    discovery.add(TableSchema(
                        database=database,
                        schema=schema,
                        table_name=current_table,
                        columns=current_cols,
                    ))
                current_table = tbl
                current_cols = []

            col_info: Dict[str, str] = {
                "name": row["COLUMN_NAME"],
                "data_type": row["DATA_TYPE"],
            }
            if row.get("CHARACTER_MAXIMUM_LENGTH"):
                col_info["max_length"] = str(row["CHARACTER_MAXIMUM_LENGTH"])
            if row.get("NUMERIC_PRECISION"):
                col_info["precision"] = str(row["NUMERIC_PRECISION"])
            if row.get("NUMERIC_SCALE") is not None:
                col_info["scale"] = str(row["NUMERIC_SCALE"])
            current_cols.append(col_info)

        # Don't forget the last table
        if current_table is not None:
            discovery.add(TableSchema(
                database=database,
                schema=schema,
                table_name=current_table,
                columns=current_cols,
            ))

        logger.info(
            "Discovered %d tables from %s.%s",
            discovery.table_count, database, schema,
        )
        return discovery

    # ------------------------------------------------------------------
    # From parsed XML
    # ------------------------------------------------------------------

    @classmethod
    def from_xml_repository(cls, repository: Any) -> SchemaDiscovery:
        """Extract source/target schemas from a parsed Informatica Repository.

        Args:
            repository: A ``Repository`` object from the XML parser.

        Returns:
            A populated :class:`SchemaDiscovery` instance.
        """
        discovery = cls()

        for folder in repository.folders:
            # Sources
            for src in folder.sources:
                cols = [
                    {"name": f.name, "data_type": f.datatype or ""}
                    for f in getattr(src, "fields", [])
                ]
                discovery.add(TableSchema(
                    database=getattr(src, "db_name", ""),
                    schema=getattr(src, "owner_name", ""),
                    table_name=src.name,
                    columns=cols,
                ))

            # Targets
            for tgt in folder.targets:
                cols = [
                    {"name": f.name, "data_type": f.datatype or ""}
                    for f in getattr(tgt, "fields", [])
                ]
                discovery.add(TableSchema(
                    database=getattr(tgt, "db_name", ""),
                    schema=getattr(tgt, "owner_name", ""),
                    table_name=tgt.name,
                    columns=cols,
                ))

        logger.info(
            "Discovered %d tables from XML repository '%s'",
            discovery.table_count, getattr(repository, "name", "unknown"),
        )
        return discovery

    # ------------------------------------------------------------------
    # From JSON file (manual fallback)
    # ------------------------------------------------------------------

    @classmethod
    def from_json(cls, path: str) -> SchemaDiscovery:
        """Load source schemas from a JSON file.

        Expected format::

            {
                "TABLE_NAME": {
                    "database": "DB",
                    "schema": "SCHEMA",
                    "columns": [{"name": "COL", "data_type": "VARCHAR"}, ...]
                }
            }
        """
        discovery = cls()
        raw = Path(path).read_text(encoding="utf-8")
        data = json.loads(raw)

        for table_name, info in data.items():
            discovery.add(TableSchema(
                database=info.get("database", ""),
                schema=info.get("schema", ""),
                table_name=table_name,
                columns=info.get("columns", []),
            ))

        logger.info(
            "Loaded %d tables from %s", discovery.table_count, path
        )
        return discovery
