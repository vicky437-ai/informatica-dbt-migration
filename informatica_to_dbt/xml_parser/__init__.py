"""XML parsing: Informatica PowerCenter metadata extraction."""

from informatica_to_dbt.xml_parser.dependency_graph import (
    build_instance_graph,
    detect_cycles,
    get_longest_path_length,
    get_source_instances,
    get_target_instances,
    get_topological_order,
    get_transformation_chains,
)
from informatica_to_dbt.xml_parser.models import (
    Connector,
    Folder,
    Instance,
    Mapping,
    Repository,
    SessionConfig,
    Shortcut,
    Source,
    TableAttribute,
    Target,
    Task,
    TransformField,
    Transformation,
)
from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

__all__ = [
    "Connector",
    "Folder",
    "InformaticaXMLParser",
    "Instance",
    "Mapping",
    "Repository",
    "SessionConfig",
    "Shortcut",
    "Source",
    "TableAttribute",
    "Target",
    "Task",
    "TransformField",
    "Transformation",
    "build_instance_graph",
    "detect_cycles",
    "get_longest_path_length",
    "get_source_instances",
    "get_target_instances",
    "get_topological_order",
    "get_transformation_chains",
]
