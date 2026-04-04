"""XML parser for Informatica PowerCenter XML exports.

Replaces the regex-based parsing from the original notebook with proper
ElementTree-driven extraction that captures all metadata, including expression
logic, lookup conditions, field-level details, and cross-folder shortcuts.

Uses ``xml.etree.ElementTree`` (stdlib) by default.  If ``lxml`` is
available (e.g. in Snowflake Notebook) it will be used automatically for
its superior error recovery.
"""

from __future__ import annotations

import html
import logging
from typing import Dict, List, Optional
from xml.etree import ElementTree as _stdlib_ET

try:
    from lxml import etree as _lxml_etree  # type: ignore[import-untyped]
    _HAS_LXML = True
except ImportError:
    _HAS_LXML = False

from informatica_to_dbt.exceptions import XMLParseError
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

logger = logging.getLogger("informatica_dbt")

# Type alias — works with both stdlib and lxml Element types
Element = _stdlib_ET.Element


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _attr(el: Element, name: str, default: str = "") -> str:
    """Get an XML attribute value, stripping surrounding whitespace."""
    val = el.get(name, default)
    return val.strip() if val else default


def _int_attr(el: Element, name: str, default: int = 0) -> int:
    """Get an XML attribute as integer."""
    raw = el.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _decode_expression(raw: Optional[str]) -> Optional[str]:
    """Decode XML-escaped Informatica expressions (``&apos;``, ``&#xD;&#xA;``)."""
    if not raw:
        return raw
    return html.unescape(raw).replace("\r\n", "\n").replace("\r", "\n")


def _localname(tag: str) -> str:
    """Strip namespace prefix from an element tag, if present.

    Works for both stdlib ``{ns}tag`` format and plain tags.
    """
    if tag and tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag or ""


# ---------------------------------------------------------------------------
# Field parsing (shared by SOURCE, TARGET, TRANSFORMATION)
# ---------------------------------------------------------------------------

def _parse_source_field(el: Element) -> TransformField:
    return TransformField(
        name=_attr(el, "NAME"),
        datatype=_attr(el, "DATATYPE"),
        precision=_int_attr(el, "PRECISION"),
        scale=_int_attr(el, "SCALE"),
        port_type=_attr(el, "KEYTYPE", "NOT A KEY"),
        description=_attr(el, "DESCRIPTION"),
    )


def _parse_transform_field(el: Element) -> TransformField:
    return TransformField(
        name=_attr(el, "NAME"),
        datatype=_attr(el, "DATATYPE"),
        precision=_int_attr(el, "PRECISION"),
        scale=_int_attr(el, "SCALE"),
        port_type=_attr(el, "PORTTYPE"),
        expression=_decode_expression(el.get("EXPRESSION")),
        default_value=_decode_expression(el.get("DEFAULTVALUE")),
        description=_attr(el, "DESCRIPTION"),
        group=el.get("GROUP"),
        ref_field=el.get("REF_FIELD"),
    )


def _parse_target_field(el: Element) -> TransformField:
    return TransformField(
        name=_attr(el, "NAME"),
        datatype=_attr(el, "DATATYPE"),
        precision=_int_attr(el, "PRECISION"),
        scale=_int_attr(el, "SCALE"),
        port_type=_attr(el, "KEYTYPE", "NOT A KEY"),
        description=_attr(el, "DESCRIPTION"),
    )


# ---------------------------------------------------------------------------
# Element parsers
# ---------------------------------------------------------------------------

def _parse_table_attributes(parent: Element) -> Dict[str, str]:
    """Extract all TABLEATTRIBUTE children as a dict."""
    attrs: Dict[str, str] = {}
    for ta in parent.findall("TABLEATTRIBUTE"):
        name = _attr(ta, "NAME")
        value = _decode_expression(ta.get("VALUE")) or ""
        if name:
            attrs[name] = value
    return attrs


def _parse_source(el: Element) -> Source:
    return Source(
        name=_attr(el, "NAME"),
        database_type=_attr(el, "DATABASETYPE"),
        db_name=_attr(el, "DBDNAME"),
        owner_name=_attr(el, "OWNERNAME"),
        fields=[_parse_source_field(f) for f in el.findall("SOURCEFIELD")],
        description=_attr(el, "DESCRIPTION"),
    )


def _parse_target(el: Element) -> Target:
    return Target(
        name=_attr(el, "NAME"),
        database_type=_attr(el, "DATABASETYPE"),
        db_name=_attr(el, "DBDNAME"),
        owner_name=_attr(el, "OWNERNAME"),
        fields=[_parse_target_field(f) for f in el.findall("TARGETFIELD")],
        description=_attr(el, "DESCRIPTION"),
    )


def _parse_transformation(el: Element) -> Transformation:
    groups = [_attr(g, "NAME") for g in el.findall("GROUP")]
    return Transformation(
        name=_attr(el, "NAME"),
        type=_attr(el, "TYPE"),
        fields=[_parse_transform_field(f) for f in el.findall("TRANSFORMFIELD")],
        attributes=_parse_table_attributes(el),
        description=_attr(el, "DESCRIPTION"),
        is_reusable=_attr(el, "REUSABLE").upper() == "YES",
        version=_int_attr(el, "VERSIONNUMBER", 1),
        groups=groups,
    )


def _parse_instance(el: Element) -> Instance:
    assoc = el.find("ASSOCIATED_SOURCE_INSTANCE")
    return Instance(
        name=_attr(el, "NAME"),
        transformation_name=_attr(el, "TRANSFORMATION_NAME"),
        transformation_type=_attr(el, "TRANSFORMATION_TYPE"),
        instance_type=_attr(el, "TYPE"),
        description=_attr(el, "DESCRIPTION"),
        associated_source=_attr(assoc, "NAME") if assoc is not None else None,
    )


def _parse_connector(el: Element) -> Connector:
    return Connector(
        from_field=_attr(el, "FROMFIELD"),
        from_instance=_attr(el, "FROMINSTANCE"),
        from_instance_type=_attr(el, "FROMINSTANCETYPE"),
        to_field=_attr(el, "TOFIELD"),
        to_instance=_attr(el, "TOINSTANCE"),
        to_instance_type=_attr(el, "TOINSTANCETYPE"),
    )


def _parse_shortcut(el: Element) -> Shortcut:
    return Shortcut(
        name=_attr(el, "NAME"),
        shortcut_to=_attr(el, "REFOBJECTNAME", _attr(el, "OBJECTSUBTYPE")),
        reference_type=_attr(el, "OBJECTSUBTYPE"),
        folder_name=_attr(el, "FOLDERNAME"),
        repository=_attr(el, "REPOSITORYNAME"),
    )


def _parse_task(el: Element) -> Task:
    return Task(
        name=_attr(el, "NAME"),
        type=_attr(el, "TYPE"),
        description=_attr(el, "DESCRIPTION"),
        attributes=_parse_table_attributes(el),
    )


def _parse_mapping(el: Element) -> Mapping:
    return Mapping(
        name=_attr(el, "NAME"),
        description=_attr(el, "DESCRIPTION"),
        is_valid=_attr(el, "ISVALID").upper() != "NO",
        sources=[_parse_source(s) for s in el.findall("SOURCE")],
        targets=[_parse_target(t) for t in el.findall("TARGET")],
        transformations=[_parse_transformation(t) for t in el.findall("TRANSFORMATION")],
        instances=[_parse_instance(i) for i in el.findall("INSTANCE")],
        connectors=[_parse_connector(c) for c in el.findall("CONNECTOR")],
        shortcuts=[_parse_shortcut(s) for s in el.findall("SHORTCUT")],
    )


def _parse_session_config(wf_el: Element) -> List[SessionConfig]:
    """Extract SESSION elements from a WORKFLOW."""
    configs: List[SessionConfig] = []
    for sess in wf_el.findall(".//SESSION"):
        cfg = SessionConfig(
            session_name=_attr(sess, "NAME"),
            mapping_name=_attr(sess, "MAPPINGNAME"),
            attributes=_parse_table_attributes(sess),
        )
        # Collect connection info from SESSTRANSFORMATIONINST
        for sti in sess.findall(".//SESSTRANSFORMATIONINST"):
            inst_name = _attr(sti, "SINSTANCENAME")
            for attr_el in sti.findall("ATTRIBUTE"):
                if _attr(attr_el, "NAME") == "Connection Information":
                    cfg.connection_info[inst_name] = _attr(attr_el, "VALUE")
        configs.append(cfg)
    return configs


def _parse_folder(el: Element) -> Folder:
    shared_val = _attr(el, "SHARED").upper()
    folder = Folder(
        name=_attr(el, "NAME"),
        shared=shared_val == "SHARED",
        description=_attr(el, "DESCRIPTION"),
    )
    folder.sources = [_parse_source(s) for s in el.findall("SOURCE")]
    folder.targets = [_parse_target(t) for t in el.findall("TARGET")]
    folder.transformations = [_parse_transformation(t) for t in el.findall("TRANSFORMATION")]
    folder.shortcuts = [_parse_shortcut(s) for s in el.findall("SHORTCUT")]
    folder.tasks = [_parse_task(t) for t in el.findall("TASK")]
    folder.mappings = [_parse_mapping(m) for m in el.findall("MAPPING")]
    folder.mapplets = [_parse_mapping(m) for m in el.findall("MAPPLET")]

    # Extract session configs from any workflow in this folder
    for wf in el.findall("WORKFLOW"):
        folder.session_configs.extend(_parse_session_config(wf))

    return folder


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class InformaticaXMLParser:
    """Parse an Informatica PowerCenter XML export into rich dataclass models.

    Usage::

        parser = InformaticaXMLParser()
        repo = parser.parse(xml_string)
        for folder in repo.folders:
            for mapping in folder.mappings:
                print(mapping.name, len(mapping.connectors))
    """

    def parse(self, xml_content: str | bytes) -> Repository:
        """Parse a complete Informatica XML export string.

        Returns a :class:`Repository` containing all folders, mappings,
        transformations, connectors, sources, targets, and shortcuts.

        Uses lxml if available (better error recovery), falls back to
        stdlib ``xml.etree.ElementTree``.

        Raises :class:`XMLParseError` on malformed XML.
        """
        if isinstance(xml_content, str):
            xml_bytes = xml_content.encode("utf-8")
        else:
            xml_bytes = xml_content

        root = self._parse_xml_bytes(xml_bytes)

        # Determine root tag (strip namespace if any)
        tag = _localname(root.tag)
        if tag != "POWERMART":
            if tag in ("REPOSITORY", "FOLDER"):
                logger.warning("XML root is <%s>, expected <POWERMART>. Adapting.", tag)
            else:
                raise XMLParseError(f"Unexpected root element: <{tag}>")

        creation_date = _attr(root, "CREATION_DATE")

        repo_el = root.find("REPOSITORY") if tag == "POWERMART" else root
        if repo_el is None:
            raise XMLParseError("No REPOSITORY element found in XML")

        repo = Repository(
            name=_attr(repo_el, "NAME"),
            database_type=_attr(repo_el, "DATABASETYPE"),
            codepage=_attr(repo_el, "CODEPAGE"),
            creation_date=creation_date,
        )

        for folder_el in repo_el.findall("FOLDER"):
            folder = _parse_folder(folder_el)
            repo.folders.append(folder)
            logger.debug(
                "Parsed folder '%s': %d sources, %d targets, %d mappings, "
                "%d transformations, %d shortcuts",
                folder.name,
                len(folder.sources),
                len(folder.targets),
                len(folder.mappings) + len(folder.mapplets),
                len(folder.transformations),
                len(folder.shortcuts),
            )

        total_mappings = sum(len(f.mappings) + len(f.mapplets) for f in repo.folders)
        total_transforms = sum(
            len(f.transformations)
            + sum(len(m.transformations) for m in f.mappings)
            + sum(len(m.transformations) for m in f.mapplets)
            for f in repo.folders
        )
        logger.info(
            "Parsed repository '%s': %d folders, %d mappings, %d transformations",
            repo.name, len(repo.folders), total_mappings, total_transforms,
        )
        return repo

    def parse_file(self, file_path: str) -> Repository:
        """Parse an Informatica XML file from the local filesystem."""
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            return self.parse(content)
        except OSError as exc:
            raise XMLParseError(f"Cannot read file '{file_path}': {exc}") from exc

    # ------------------------------------------------------------------
    # Internal: XML byte-string → Element root
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_xml_bytes(xml_bytes: bytes) -> Element:
        """Parse raw XML bytes into an Element tree root.

        Tries lxml first (if available) for its ``recover=True`` error
        tolerance, then falls back to stdlib ElementTree.
        """
        if _HAS_LXML:
            try:
                lxml_parser = _lxml_etree.XMLParser(recover=True, encoding="utf-8")
                lxml_root = _lxml_etree.fromstring(xml_bytes, parser=lxml_parser)
                # Convert lxml element to stdlib element for a uniform API.
                # For large files this is negligible compared to LLM calls.
                return _stdlib_ET.fromstring(_lxml_etree.tostring(lxml_root))
            except Exception as exc:
                logger.warning("lxml parsing failed, falling back to stdlib: %s", exc)

        # stdlib fallback — strip DOCTYPE declarations to mitigate XXE attacks
        # (H1 fix).  stdlib ElementTree's expat parser processes entity
        # declarations by default; stripping the DOCTYPE block removes any
        # ENTITY definitions before the parser sees them.
        # The internal-subset pattern handles ] inside quoted strings
        # (e.g. <!ENTITY x "a]b">) by consuming "..." and '...' spans.
        try:
            import re as _xxe_re
            cleaned = _xxe_re.sub(
                rb'<!DOCTYPE[^>\[]*(\[(?:[^\]"\']|"[^"]*"|\'[^\']*\')*\])?\s*>',
                b'',
                xml_bytes,
                flags=_xxe_re.IGNORECASE | _xxe_re.DOTALL,
            )
            return _stdlib_ET.fromstring(cleaned)
        except _stdlib_ET.ParseError as exc:
            raise XMLParseError(f"Failed to parse XML: {exc}") from exc
