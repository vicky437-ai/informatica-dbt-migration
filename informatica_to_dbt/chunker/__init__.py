"""Context-preserving XML chunking for LLM token limits."""

from informatica_to_dbt.chunker.context_preserving import (
    MappingChunk,
    chunk_mapping,
)

__all__ = ["MappingChunk", "chunk_mapping"]
