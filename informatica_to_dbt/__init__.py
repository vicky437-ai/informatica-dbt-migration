"""Informatica PowerCenter XML to dbt project converter for Snowflake."""

__version__ = "1.0.0"

from informatica_to_dbt.config import Config
from informatica_to_dbt.exceptions import ErrorCategory, classify_error
from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics
from informatica_to_dbt.orchestrator import (
    MappingResult,
    convert_and_merge,
    convert_mapping,
    convert_repository,
    retry_failed_mappings,
)
from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

__all__ = [
    "Config",
    "ErrorCategory",
    "InformaticaXMLParser",
    "MappingMetrics",
    "MappingResult",
    "RepositoryMetrics",
    "classify_error",
    "convert_and_merge",
    "convert_mapping",
    "convert_repository",
    "retry_failed_mappings",
]
