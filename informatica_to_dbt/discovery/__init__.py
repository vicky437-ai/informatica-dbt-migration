"""Discovery module — XML inventory and Snowflake schema discovery."""

from informatica_to_dbt.discovery.xml_inventory import XMLInventory, XMLFileInfo
from informatica_to_dbt.discovery.schema_discovery import SchemaDiscovery

__all__ = ["XMLInventory", "XMLFileInfo", "SchemaDiscovery"]
