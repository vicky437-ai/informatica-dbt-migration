"""Unit tests for XML parser."""

import pytest
from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser
from informatica_to_dbt.exceptions import XMLParseError


MINIMAL_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<POWERMART CREATION_DATE="01/01/2024">
  <REPOSITORY NAME="TEST_REPO" DATABASETYPE="Oracle" CODEPAGE="UTF-8">
    <FOLDER NAME="TestFolder" SHARED="NOTSHARED">
      <SOURCE NAME="SRC_T" DATABASETYPE="Oracle" DBDNAME="PROD" OWNERNAME="APP">
        <SOURCEFIELD NAME="COL_A" DATATYPE="varchar2" PRECISION="50" SCALE="0" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="COL_B" DATATYPE="number" PRECISION="10" SCALE="2" KEYTYPE="PRIMARY KEY"/>
      </SOURCE>
      <TARGET NAME="TGT_T" DATABASETYPE="Oracle" DBDNAME="DW" OWNERNAME="STG">
        <TARGETFIELD NAME="COL_A" DATATYPE="varchar2" PRECISION="50" SCALE="0"/>
      </TARGET>
      <MAPPING NAME="m_test" ISVALID="YES">
        <TRANSFORMATION NAME="SQ_SRC" TYPE="Source Qualifier" REUSABLE="NO">
          <TRANSFORMFIELD NAME="COL_A" DATATYPE="varchar2" PRECISION="50" PORTTYPE="INPUT/OUTPUT"/>
          <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT COL_A FROM SRC_T"/>
        </TRANSFORMATION>
        <TRANSFORMATION NAME="EXP_CALC" TYPE="Expression" REUSABLE="NO">
          <TRANSFORMFIELD NAME="COL_A" DATATYPE="varchar2" PRECISION="50" PORTTYPE="INPUT/OUTPUT" EXPRESSION="UPPER(COL_A)"/>
        </TRANSFORMATION>
        <INSTANCE NAME="SRC_T" TRANSFORMATION_NAME="SRC_T" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
        <INSTANCE NAME="SQ_SRC" TRANSFORMATION_NAME="SQ_SRC" TRANSFORMATION_TYPE="Source Qualifier"/>
        <INSTANCE NAME="EXP_CALC" TRANSFORMATION_NAME="EXP_CALC" TRANSFORMATION_TYPE="Expression"/>
        <INSTANCE NAME="TGT_T" TRANSFORMATION_NAME="TGT_T" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
        <CONNECTOR FROMFIELD="COL_A" FROMINSTANCE="SRC_T" FROMINSTANCETYPE="Source Definition"
                   TOFIELD="COL_A" TOINSTANCE="SQ_SRC" TOINSTANCETYPE="Source Qualifier"/>
        <CONNECTOR FROMFIELD="COL_A" FROMINSTANCE="SQ_SRC" FROMINSTANCETYPE="Source Qualifier"
                   TOFIELD="COL_A" TOINSTANCE="EXP_CALC" TOINSTANCETYPE="Expression"/>
        <CONNECTOR FROMFIELD="COL_A" FROMINSTANCE="EXP_CALC" FROMINSTANCETYPE="Expression"
                   TOFIELD="COL_A" TOINSTANCE="TGT_T" TOINSTANCETYPE="Target Definition"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""


class TestInformaticaXMLParser:
    """Tests for InformaticaXMLParser.parse()."""

    def test_parse_minimal_xml(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)

        assert repo.name == "TEST_REPO"
        assert repo.database_type == "Oracle"
        assert len(repo.folders) == 1

    def test_folder_contents(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        folder = repo.folders[0]

        assert folder.name == "TestFolder"
        assert folder.shared is False
        assert len(folder.sources) == 1
        assert len(folder.targets) == 1
        assert len(folder.mappings) == 1

    def test_source_parsing(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        src = repo.folders[0].sources[0]

        assert src.name == "SRC_T"
        assert src.database_type == "Oracle"
        assert src.db_name == "PROD"
        assert src.owner_name == "APP"
        assert len(src.fields) == 2
        assert src.fields[0].name == "COL_A"
        assert src.fields[1].precision == 10
        assert src.fields[1].scale == 2

    def test_target_parsing(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        tgt = repo.folders[0].targets[0]

        assert tgt.name == "TGT_T"
        assert tgt.database_type == "Oracle"
        assert len(tgt.fields) == 1

    def test_mapping_parsing(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        mapping = repo.folders[0].mappings[0]

        assert mapping.name == "m_test"
        assert mapping.is_valid is True
        assert len(mapping.transformations) == 2
        assert len(mapping.instances) == 4
        assert len(mapping.connectors) == 3

    def test_transformation_details(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        mapping = repo.folders[0].mappings[0]

        sq = mapping.transformations[0]
        assert sq.name == "SQ_SRC"
        assert sq.type == "Source Qualifier"
        assert sq.attributes.get("Sql Query") == "SELECT COL_A FROM SRC_T"

        exp = mapping.transformations[1]
        assert exp.name == "EXP_CALC"
        assert exp.type == "Expression"
        assert exp.fields[0].expression == "UPPER(COL_A)"

    def test_connector_parsing(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        conn = repo.folders[0].mappings[0].connectors[0]

        assert conn.from_field == "COL_A"
        assert conn.from_instance == "SRC_T"
        assert conn.to_instance == "SQ_SRC"

    def test_instance_parsing(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML)
        instances = repo.folders[0].mappings[0].instances

        source_inst = instances[0]
        assert source_inst.name == "SRC_T"
        assert source_inst.instance_type == "SOURCE"

    def test_invalid_xml_raises_error(self):
        parser = InformaticaXMLParser()
        with pytest.raises(XMLParseError):
            parser.parse("<NOTVALID>broken xml")

    def test_unexpected_root_raises_error(self):
        parser = InformaticaXMLParser()
        with pytest.raises(XMLParseError):
            parser.parse("<RANDOM_ROOT/>")

    def test_parse_bytes(self):
        parser = InformaticaXMLParser()
        repo = parser.parse(MINIMAL_XML.encode("utf-8"))
        assert repo.name == "TEST_REPO"

    def test_html_entity_decoding(self):
        xml = MINIMAL_XML.replace(
            'EXPRESSION="UPPER(COL_A)"',
            'EXPRESSION="IIF(COL_A = &apos;X&apos;, 1, 0)"',
        )
        parser = InformaticaXMLParser()
        repo = parser.parse(xml)
        exp = repo.folders[0].mappings[0].transformations[1]
        assert "IIF(COL_A = 'X', 1, 0)" == exp.fields[0].expression

    def test_shortcut_parsing(self):
        xml_with_shortcut = MINIMAL_XML.replace(
            "</MAPPING>",
            "</MAPPING>\n"
            '<SHORTCUT NAME="SC_SRC" REFOBJECTNAME="SRC_SHARED" '
            'OBJECTSUBTYPE="Source Definition" FOLDERNAME="SharedFolder" '
            'REPOSITORYNAME="REPO"/>',
        )
        parser = InformaticaXMLParser()
        repo = parser.parse(xml_with_shortcut)
        assert len(repo.folders[0].shortcuts) == 1
        sc = repo.folders[0].shortcuts[0]
        assert sc.name == "SC_SRC"
        assert sc.shortcut_to == "SRC_SHARED"
        assert sc.folder_name == "SharedFolder"
