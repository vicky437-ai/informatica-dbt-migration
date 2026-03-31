"""Discover and inventory Informatica PowerCenter XML files.

Scans a directory (recursively) for XML files that look like Informatica
PowerCenter exports, extracts key metadata (repository name, folders,
mapping count, source/target counts), and produces a structured inventory.

Usage::

    inventory = XMLInventory.scan("/path/to/xmls")
    for info in inventory.files:
        print(f"{info.filename}: {info.mapping_count} mappings")
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from lxml import etree

logger = logging.getLogger("informatica_dbt")


@dataclass
class XMLFileInfo:
    """Metadata extracted from a single Informatica XML file."""

    path: str
    filename: str
    size_bytes: int = 0
    sha256: str = ""
    is_valid_informatica: bool = False
    repository_name: str = ""
    database_type: str = ""
    folder_count: int = 0
    mapping_count: int = 0
    source_count: int = 0
    target_count: int = 0
    transformation_count: int = 0
    folder_names: List[str] = field(default_factory=list)
    mapping_names: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class XMLInventory:
    """Collection of discovered XML files with summary statistics."""

    root_dir: str
    files: List[XMLFileInfo] = field(default_factory=list)

    @property
    def total_mappings(self) -> int:
        return sum(f.mapping_count for f in self.files if f.is_valid_informatica)

    @property
    def total_sources(self) -> int:
        return sum(f.source_count for f in self.files if f.is_valid_informatica)

    @property
    def total_targets(self) -> int:
        return sum(f.target_count for f in self.files if f.is_valid_informatica)

    @property
    def valid_files(self) -> List[XMLFileInfo]:
        return [f for f in self.files if f.is_valid_informatica]

    @property
    def invalid_files(self) -> List[XMLFileInfo]:
        return [f for f in self.files if not f.is_valid_informatica]

    def summary(self) -> str:
        valid = len(self.valid_files)
        invalid = len(self.invalid_files)
        return (
            f"Discovered {valid} valid Informatica XML file(s) "
            f"({self.total_mappings} mappings, "
            f"{self.total_sources} sources, "
            f"{self.total_targets} targets)"
            + (f" + {invalid} invalid file(s)" if invalid else "")
        )

    @classmethod
    def scan(cls, root_dir: str) -> XMLInventory:
        """Scan a directory for Informatica XML files.

        Args:
            root_dir: Directory path to scan (recursively).

        Returns:
            An :class:`XMLInventory` with metadata for each file found.
        """
        root = Path(root_dir)
        inventory = cls(root_dir=str(root))

        if root.is_file():
            # Single file mode
            inventory.files.append(_inspect_xml(root))
            return inventory

        if not root.is_dir():
            logger.warning("Scan path does not exist: %s", root_dir)
            return inventory

        xml_paths = sorted(root.rglob("*.xml")) + sorted(root.rglob("*.XML"))
        # Deduplicate (case-insensitive filesystems may return duplicates)
        seen: set = set()
        for p in xml_paths:
            resolved = str(p.resolve())
            if resolved not in seen:
                seen.add(resolved)
                inventory.files.append(_inspect_xml(p))

        logger.info(inventory.summary())
        return inventory


def _inspect_xml(path: Path) -> XMLFileInfo:
    """Extract metadata from a single XML file without full parsing."""
    info = XMLFileInfo(
        path=str(path),
        filename=path.name,
    )

    try:
        raw = path.read_bytes()
        info.size_bytes = len(raw)
        info.sha256 = hashlib.sha256(raw).hexdigest()
    except OSError as exc:
        info.error = f"Read error: {exc}"
        return info

    try:
        # Use iterparse for efficiency — only read top-level elements
        tree = etree.fromstring(raw)
    except etree.XMLSyntaxError as exc:
        info.error = f"XML parse error: {exc}"
        return info

    # Check if this is an Informatica PowerCenter XML
    root_tag = etree.QName(tree.tag).localname if "}" in tree.tag else tree.tag
    if root_tag not in ("POWERMART", "REPOSITORY"):
        info.error = f"Not Informatica XML (root element: {root_tag})"
        return info

    info.is_valid_informatica = True
    info.repository_name = tree.get("REPOSITORY_NAME", "")
    info.database_type = tree.get("DATABASE_TYPE", "")

    # Count elements
    folders = tree.findall(".//FOLDER")
    info.folder_count = len(folders)
    info.folder_names = [f.get("NAME", "") for f in folders]

    mappings = tree.findall(".//MAPPING")
    info.mapping_count = len(mappings)
    info.mapping_names = [m.get("NAME", "") for m in mappings]

    info.source_count = len(tree.findall(".//SOURCE"))
    info.target_count = len(tree.findall(".//TARGET"))
    info.transformation_count = len(tree.findall(".//TRANSFORMATION"))

    return info
