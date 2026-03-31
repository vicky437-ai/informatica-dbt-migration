"""Build a networkx directed graph from parsed Informatica mapping metadata.

The graph nodes are *instance names* (from INSTANCE elements within a MAPPING).
Edges represent field-level data flow derived from CONNECTOR elements.
This graph powers dependency-aware chunking and complexity analysis.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple

import networkx as nx

from informatica_to_dbt.xml_parser.models import Connector, Instance, Mapping

logger = logging.getLogger("informatica_dbt")


def build_instance_graph(mapping: Mapping) -> nx.DiGraph:
    """Build a directed graph of instance-level data flow for a single mapping.

    Nodes carry metadata:
        - ``type``: transformation type (Source Qualifier, Expression, …)
        - ``transformation_name``: the referenced transformation definition
        - ``instance_type``: SOURCE / TARGET / TRANSFORMATION

    Edges carry metadata:
        - ``fields``: list of ``(from_field, to_field)`` tuples
    """
    g = nx.DiGraph()

    # Add nodes from instances
    inst_map: Dict[str, Instance] = {}
    for inst in mapping.instances:
        inst_map[inst.name] = inst
        g.add_node(
            inst.name,
            type=inst.transformation_type,
            transformation_name=inst.transformation_name,
            instance_type=inst.instance_type,
        )

    # Add edges from connectors (deduplicate instance-level edges)
    edge_fields: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for conn in mapping.connectors:
        key = (conn.from_instance, conn.to_instance)
        edge_fields[key].append((conn.from_field, conn.to_field))
        # Ensure nodes exist even if no INSTANCE element was found
        if conn.from_instance not in g:
            g.add_node(conn.from_instance, type=conn.from_instance_type,
                       transformation_name="", instance_type="")
        if conn.to_instance not in g:
            g.add_node(conn.to_instance, type=conn.to_instance_type,
                       transformation_name="", instance_type="")

    for (src, dst), fields in edge_fields.items():
        g.add_edge(src, dst, fields=fields)

    logger.debug(
        "Built instance graph for mapping '%s': %d nodes, %d edges",
        mapping.name, g.number_of_nodes(), g.number_of_edges(),
    )
    return g


def get_topological_order(g: nx.DiGraph) -> List[str]:
    """Return nodes in topological order; fall back to DFS if cycles exist."""
    try:
        return list(nx.topological_sort(g))
    except nx.NetworkXUnfeasible:
        logger.warning("Cycle detected in dependency graph — using DFS order")
        return list(nx.dfs_preorder_nodes(g))


def get_transformation_chains(g: nx.DiGraph) -> List[List[str]]:
    """Find weakly connected components and return each as a topo-sorted chain.

    Each chain represents an independent data pipeline within the mapping.
    """
    chains: List[List[str]] = []
    for component in nx.weakly_connected_components(g):
        sub = g.subgraph(component)
        try:
            chain = list(nx.topological_sort(sub))
        except nx.NetworkXUnfeasible:
            chain = list(nx.dfs_preorder_nodes(sub))
            logger.warning("Cycle in component %s — using DFS order", component)
        chains.append(chain)
    return chains


def get_source_instances(g: nx.DiGraph) -> List[str]:
    """Return instance names that are graph roots (in-degree 0)."""
    return [n for n in g.nodes if g.in_degree(n) == 0]


def get_target_instances(g: nx.DiGraph) -> List[str]:
    """Return instance names that are graph leaves (out-degree 0)."""
    return [n for n in g.nodes if g.out_degree(n) == 0]


def get_longest_path_length(g: nx.DiGraph) -> int:
    """Return the length of the longest path in the DAG (0 if cyclic)."""
    try:
        return nx.dag_longest_path_length(g)
    except nx.NetworkXUnfeasible:
        return 0


def get_instance_predecessors(g: nx.DiGraph, instance_name: str) -> Set[str]:
    """Return all ancestor instance names (transitive predecessors)."""
    return nx.ancestors(g, instance_name) if instance_name in g else set()


def get_instance_successors(g: nx.DiGraph, instance_name: str) -> Set[str]:
    """Return all descendant instance names (transitive successors)."""
    return nx.descendants(g, instance_name) if instance_name in g else set()


def detect_cycles(g: nx.DiGraph) -> List[List[str]]:
    """Return all simple cycles in the graph (empty list if DAG)."""
    try:
        return list(nx.simple_cycles(g))
    except Exception:
        return []
