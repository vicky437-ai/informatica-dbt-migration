"""Live dbt validation — runs ``dbt compile`` and ``dbt run`` as subprocesses."""

from informatica_to_dbt.validation.dbt_validator import DbtValidator, DbtValidationResult

__all__ = ["DbtValidator", "DbtValidationResult"]
