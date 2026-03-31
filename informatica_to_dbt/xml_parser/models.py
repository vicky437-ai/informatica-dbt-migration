"""Dataclass models representing Informatica PowerCenter XML elements."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TransformField:
    """A single port / field within a TRANSFORMATION, SOURCE, or TARGET."""

    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    port_type: str = ""          # INPUT, OUTPUT, INPUT/OUTPUT, LOCAL VARIABLE, LOOKUP, LOOKUP/OUTPUT
    expression: Optional[str] = None
    default_value: Optional[str] = None
    description: str = ""
    group: Optional[str] = None  # For Router / Union groups
    ref_field: Optional[str] = None  # For Router output field references


@dataclass
class TableAttribute:
    """A TABLEATTRIBUTE key/value pair (SQL override, lookup condition, etc.)."""

    name: str
    value: str


@dataclass
class Transformation:
    """An Informatica TRANSFORMATION element."""

    name: str
    type: str                    # Source Qualifier, Expression, Lookup Procedure, Router, etc.
    fields: List[TransformField] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)  # TABLEATTRIBUTE name→value
    description: str = ""
    is_reusable: bool = False
    version: int = 1
    groups: List[str] = field(default_factory=list)  # GROUP names (Router, Union)


@dataclass
class Source:
    """An Informatica SOURCE element (database table definition)."""

    name: str
    database_type: str = ""      # Oracle, Microsoft SQL Server, Flat File, etc.
    db_name: str = ""            # DBDNAME
    owner_name: str = ""         # OWNERNAME
    fields: List[TransformField] = field(default_factory=list)
    description: str = ""


@dataclass
class Target:
    """An Informatica TARGET element."""

    name: str
    database_type: str = ""
    db_name: str = ""
    owner_name: str = ""
    fields: List[TransformField] = field(default_factory=list)
    description: str = ""


@dataclass
class Instance:
    """An INSTANCE element within a MAPPING (maps a name to a transformation)."""

    name: str
    transformation_name: str = ""
    transformation_type: str = ""
    instance_type: str = ""      # SOURCE, TARGET, TRANSFORMATION
    description: str = ""
    associated_source: Optional[str] = None  # ASSOCIATED_SOURCE_INSTANCE


@dataclass
class Connector:
    """A CONNECTOR element describing a field-level data flow edge."""

    from_field: str
    from_instance: str
    from_instance_type: str
    to_field: str
    to_instance: str
    to_instance_type: str


@dataclass
class Shortcut:
    """A SHORTCUT element referencing an object in another (shared) folder."""

    name: str
    shortcut_to: str = ""        # Original object name
    reference_type: str = ""     # Source Definition, Target Definition, Transformation, etc.
    folder_name: str = ""        # FOLDERNAME the shortcut points to
    repository: str = ""         # REPOSITORYNAME


@dataclass
class Task:
    """A TASK element (pre/post session commands)."""

    name: str
    type: str = ""
    description: str = ""
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class SessionConfig:
    """Session-level configuration extracted from WORKFLOW → SESSION."""

    session_name: str = ""
    mapping_name: str = ""
    attributes: Dict[str, str] = field(default_factory=dict)
    connection_info: Dict[str, str] = field(default_factory=dict)  # instance → connection


@dataclass
class Mapping:
    """A complete Informatica MAPPING with all child elements."""

    name: str
    description: str = ""
    is_valid: bool = True
    sources: List[Source] = field(default_factory=list)
    targets: List[Target] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    instances: List[Instance] = field(default_factory=list)
    connectors: List[Connector] = field(default_factory=list)
    shortcuts: List[Shortcut] = field(default_factory=list)


@dataclass
class Folder:
    """A FOLDER element — the top-level grouping within a REPOSITORY."""

    name: str
    shared: bool = False
    description: str = ""
    sources: List[Source] = field(default_factory=list)
    targets: List[Target] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    mappings: List[Mapping] = field(default_factory=list)
    shortcuts: List[Shortcut] = field(default_factory=list)
    tasks: List[Task] = field(default_factory=list)
    session_configs: List[SessionConfig] = field(default_factory=list)


@dataclass
class Repository:
    """The top-level parsed Informatica export."""

    name: str
    database_type: str = ""
    codepage: str = "UTF-8"
    folders: List[Folder] = field(default_factory=list)
    creation_date: str = ""
