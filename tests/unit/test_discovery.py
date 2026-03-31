"""Unit tests for the discovery modules (XML inventory + schema discovery)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from informatica_to_dbt.discovery.xml_inventory import XMLFileInfo, XMLInventory
from informatica_to_dbt.discovery.schema_discovery import SchemaDiscovery, TableSchema


# ---------------------------------------------------------------------------
# Helpers — minimal Informatica XML for testing
# ---------------------------------------------------------------------------

MINIMAL_INFA_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="01/01/2024" REPOSITORY_VERSION="1" REPOSITORY_NAME="TEST_REPO" DATABASE_TYPE="Oracle">
  <REPOSITORY NAME="TEST_REPO" VERSION="1" DATABASETYPE="Oracle">
    <FOLDER NAME="TEST_FOLDER" GROUP="" OWNER="" SHARED="NOTSHARED" DESCRIPTION="">
      <SOURCE NAME="SRC_ORDERS" DATABASETYPE="Oracle" DBDNAME="ORCL" OWNERNAME="DBO" DESCRIPTION="">
        <SOURCEFIELD NAME="ORDER_ID" DATATYPE="number" PRECISION="10" SCALE="0" />
        <SOURCEFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" />
      </SOURCE>
      <TARGET NAME="TGT_ORDERS" DATABASETYPE="Snowflake" DBDNAME="ANALYTICS" DESCRIPTION="">
        <TARGETFIELD NAME="ORDER_ID" DATATYPE="number" PRECISION="10" SCALE="0" />
        <TARGETFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" />
      </TARGET>
      <MAPPING NAME="m_load_orders" DESCRIPTION="" ISVALID="YES">
        <TRANSFORMATION NAME="SQ_SRC_ORDERS" TYPE="Source Qualifier" DESCRIPTION="">
        </TRANSFORMATION>
        <TRANSFORMATION NAME="EXP_TRANSFORM" TYPE="Expression" DESCRIPTION="">
        </TRANSFORMATION>
      </MAPPING>
      <MAPPING NAME="m_load_customers" DESCRIPTION="" ISVALID="YES">
        <TRANSFORMATION NAME="SQ_SRC_CUST" TYPE="Source Qualifier" DESCRIPTION="">
        </TRANSFORMATION>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""

NOT_INFA_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <setting name="foo" value="bar" />
</configuration>
"""

INVALID_XML = "this is not xml at all <><><"


# ---------------------------------------------------------------------------
# XMLInventory
# ---------------------------------------------------------------------------

class TestXMLInventory:
    def test_scan_single_file(self, tmp_path):
        xml_file = tmp_path / "export.xml"
        xml_file.write_text(MINIMAL_INFA_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(xml_file))
        assert len(inventory.files) == 1
        assert inventory.files[0].is_valid_informatica
        assert inventory.files[0].mapping_count == 2
        assert inventory.files[0].source_count == 1
        assert inventory.files[0].target_count == 1
        assert inventory.files[0].transformation_count == 3
        assert inventory.files[0].repository_name == "TEST_REPO"

    def test_scan_directory(self, tmp_path):
        (tmp_path / "a.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")
        (tmp_path / "b.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(tmp_path))
        assert len(inventory.valid_files) == 2
        assert inventory.total_mappings == 4  # 2 per file

    def test_scan_filters_non_informatica(self, tmp_path):
        (tmp_path / "infa.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")
        (tmp_path / "config.xml").write_text(NOT_INFA_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(tmp_path))
        assert len(inventory.valid_files) == 1
        assert len(inventory.invalid_files) == 1
        assert "Not Informatica XML" in inventory.invalid_files[0].error

    def test_scan_handles_invalid_xml(self, tmp_path):
        (tmp_path / "bad.xml").write_text(INVALID_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(tmp_path))
        assert len(inventory.files) == 1
        assert not inventory.files[0].is_valid_informatica
        assert "XML parse error" in inventory.files[0].error

    def test_scan_empty_directory(self, tmp_path):
        inventory = XMLInventory.scan(str(tmp_path))
        assert len(inventory.files) == 0
        assert inventory.total_mappings == 0

    def test_scan_nonexistent_path(self, tmp_path):
        inventory = XMLInventory.scan(str(tmp_path / "nonexistent"))
        assert len(inventory.files) == 0

    def test_scan_recursive(self, tmp_path):
        subdir = tmp_path / "sub" / "deep"
        subdir.mkdir(parents=True)
        (subdir / "nested.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(tmp_path))
        assert len(inventory.valid_files) == 1
        assert inventory.valid_files[0].filename == "nested.xml"

    def test_file_info_metadata(self, tmp_path):
        xml_file = tmp_path / "export.xml"
        xml_file.write_text(MINIMAL_INFA_XML, encoding="utf-8")

        inventory = XMLInventory.scan(str(xml_file))
        info = inventory.files[0]
        assert info.size_bytes > 0
        assert len(info.sha256) == 64
        assert info.folder_count == 1
        assert info.folder_names == ["TEST_FOLDER"]
        assert set(info.mapping_names) == {"m_load_orders", "m_load_customers"}

    def test_summary(self, tmp_path):
        (tmp_path / "a.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")
        inventory = XMLInventory.scan(str(tmp_path))
        s = inventory.summary()
        assert "1 valid" in s
        assert "2 mappings" in s

    def test_total_sources_and_targets(self, tmp_path):
        (tmp_path / "a.xml").write_text(MINIMAL_INFA_XML, encoding="utf-8")
        inventory = XMLInventory.scan(str(tmp_path))
        assert inventory.total_sources == 1
        assert inventory.total_targets == 1


# ---------------------------------------------------------------------------
# XMLFileInfo
# ---------------------------------------------------------------------------

class TestXMLFileInfo:
    def test_defaults(self):
        info = XMLFileInfo(path="/tmp/test.xml", filename="test.xml")
        assert info.size_bytes == 0
        assert info.sha256 == ""
        assert not info.is_valid_informatica
        assert info.mapping_count == 0
        assert info.error is None


# ---------------------------------------------------------------------------
# SchemaDiscovery
# ---------------------------------------------------------------------------

class TestSchemaDiscovery:
    def test_add_and_get(self):
        sd = SchemaDiscovery()
        sd.add(TableSchema(
            database="DB", schema="RAW", table_name="ORDERS",
            columns=[{"name": "ID", "data_type": "NUMBER"}],
        ))
        assert sd.table_count == 1
        t = sd.get("ORDERS")
        assert t is not None
        assert t.database == "DB"
        assert len(t.columns) == 1

    def test_get_case_insensitive(self):
        sd = SchemaDiscovery()
        sd.add(TableSchema(table_name="ORDERS"))
        assert sd.get("orders") is not None
        assert sd.get("Orders") is not None

    def test_get_missing_returns_none(self):
        sd = SchemaDiscovery()
        assert sd.get("NONEXISTENT") is None

    def test_to_source_map(self):
        sd = SchemaDiscovery()
        sd.add(TableSchema(
            database="DB", schema="RAW", table_name="ORDERS",
            columns=[{"name": "ID", "data_type": "NUMBER"}],
        ))
        sm = sd.to_source_map()
        assert "ORDERS" in sm
        assert sm["ORDERS"]["database"] == "DB"
        assert sm["ORDERS"]["schema"] == "RAW"
        assert len(sm["ORDERS"]["columns"]) == 1

    def test_save_and_load_json(self, tmp_path):
        sd = SchemaDiscovery()
        sd.add(TableSchema(
            database="DB", schema="S", table_name="T1",
            columns=[{"name": "C1", "data_type": "VARCHAR"}],
        ))
        sd.add(TableSchema(
            database="DB", schema="S", table_name="T2",
            columns=[],
        ))

        json_path = str(tmp_path / "source_map.json")
        sd.save_json(json_path)

        loaded = SchemaDiscovery.from_json(json_path)
        assert loaded.table_count == 2
        t1 = loaded.get("T1")
        assert t1 is not None
        assert t1.columns[0]["name"] == "C1"

    def test_from_json_with_manual_file(self, tmp_path):
        data = {
            "CUSTOMERS": {
                "database": "PROD",
                "schema": "PUBLIC",
                "columns": [
                    {"name": "CUST_ID", "data_type": "NUMBER"},
                    {"name": "CUST_NAME", "data_type": "VARCHAR"},
                ],
            }
        }
        json_path = tmp_path / "manual.json"
        json_path.write_text(json.dumps(data), encoding="utf-8")

        sd = SchemaDiscovery.from_json(str(json_path))
        assert sd.table_count == 1
        c = sd.get("CUSTOMERS")
        assert c is not None
        assert len(c.columns) == 2
        assert c.database == "PROD"
