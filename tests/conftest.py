"""Shared pytest configuration and fixtures."""

import pytest
from informatica_to_dbt.config import Config
from informatica_to_dbt.xml_parser.models import (
    Connector,
    Folder,
    Instance,
    Mapping,
    Repository,
    Source,
    Target,
    TransformField,
    Transformation,
)
from informatica_to_dbt.generator.response_parser import GeneratedFile


@pytest.fixture
def config():
    """Return a default Config for testing."""
    return Config()


@pytest.fixture
def simple_mapping():
    """A minimal mapping: 1 source → SQ → Expression → 1 target."""
    return Mapping(
        name="m_simple_test",
        sources=[
            Source(
                name="SRC_ORDERS",
                database_type="Oracle",
                db_name="PROD",
                owner_name="APP",
                fields=[
                    TransformField(name="ORDER_ID", datatype="number", precision=10),
                    TransformField(name="CUST_ID", datatype="number", precision=10),
                    TransformField(name="AMOUNT", datatype="decimal", precision=10, scale=2),
                ],
            ),
        ],
        targets=[
            Target(
                name="TGT_ORDERS",
                database_type="Oracle",
                db_name="DW",
                owner_name="STG",
                fields=[
                    TransformField(name="ORDER_ID", datatype="number", precision=10),
                    TransformField(name="CUST_ID", datatype="number", precision=10),
                    TransformField(name="AMOUNT", datatype="decimal", precision=10, scale=2),
                ],
            ),
        ],
        transformations=[
            Transformation(
                name="SQ_SRC_ORDERS",
                type="Source Qualifier",
                fields=[
                    TransformField(name="ORDER_ID", datatype="number", precision=10, port_type="INPUT/OUTPUT"),
                    TransformField(name="CUST_ID", datatype="number", precision=10, port_type="INPUT/OUTPUT"),
                    TransformField(name="AMOUNT", datatype="decimal", precision=10, scale=2, port_type="INPUT/OUTPUT"),
                ],
            ),
            Transformation(
                name="EXP_TRANSFORM",
                type="Expression",
                fields=[
                    TransformField(name="ORDER_ID", datatype="number", precision=10, port_type="INPUT/OUTPUT"),
                    TransformField(name="CUST_ID", datatype="number", precision=10, port_type="INPUT/OUTPUT"),
                    TransformField(name="AMOUNT", datatype="decimal", precision=10, scale=2, port_type="INPUT/OUTPUT"),
                    TransformField(
                        name="AMOUNT_USD",
                        datatype="decimal",
                        precision=10,
                        scale=2,
                        port_type="OUTPUT",
                        expression="AMOUNT * 1.1",
                    ),
                ],
            ),
        ],
        instances=[
            Instance(name="SRC_ORDERS", transformation_name="SRC_ORDERS", transformation_type="Source Definition", instance_type="SOURCE"),
            Instance(name="SQ_SRC_ORDERS", transformation_name="SQ_SRC_ORDERS", transformation_type="Source Qualifier"),
            Instance(name="EXP_TRANSFORM", transformation_name="EXP_TRANSFORM", transformation_type="Expression"),
            Instance(name="TGT_ORDERS", transformation_name="TGT_ORDERS", transformation_type="Target Definition", instance_type="TARGET"),
        ],
        connectors=[
            Connector(from_field="ORDER_ID", from_instance="SRC_ORDERS", from_instance_type="Source Definition",
                      to_field="ORDER_ID", to_instance="SQ_SRC_ORDERS", to_instance_type="Source Qualifier"),
            Connector(from_field="ORDER_ID", from_instance="SQ_SRC_ORDERS", from_instance_type="Source Qualifier",
                      to_field="ORDER_ID", to_instance="EXP_TRANSFORM", to_instance_type="Expression"),
            Connector(from_field="ORDER_ID", from_instance="EXP_TRANSFORM", from_instance_type="Expression",
                      to_field="ORDER_ID", to_instance="TGT_ORDERS", to_instance_type="Target Definition"),
        ],
    )


@pytest.fixture
def complex_mapping():
    """A complex mapping with Router, Lookup, Update Strategy."""
    return Mapping(
        name="m_complex_test",
        sources=[
            Source(name="SRC_A", database_type="Oracle", fields=[
                TransformField(name="ID", datatype="number", precision=10),
                TransformField(name="STATUS", datatype="string", precision=50),
            ]),
            Source(name="SRC_B", database_type="Microsoft SQL Server", fields=[
                TransformField(name="ID", datatype="number", precision=10),
                TransformField(name="DESC", datatype="string", precision=100),
            ]),
        ],
        targets=[
            Target(name="TGT_DIM", database_type="Oracle", fields=[
                TransformField(name="ID", datatype="number", precision=10),
                TransformField(name="STATUS", datatype="string", precision=50),
                TransformField(name="DESC", datatype="string", precision=100),
            ]),
        ],
        transformations=[
            Transformation(name="SQ_A", type="Source Qualifier", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT"),
                TransformField(name="STATUS", datatype="string", port_type="INPUT/OUTPUT"),
            ]),
            Transformation(name="SQ_B", type="Source Qualifier", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT"),
                TransformField(name="DESC", datatype="string", port_type="INPUT/OUTPUT"),
            ]),
            Transformation(name="JNR_AB", type="Joiner", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT"),
                TransformField(name="STATUS", datatype="string", port_type="INPUT/OUTPUT"),
                TransformField(name="DESC", datatype="string", port_type="INPUT/OUTPUT"),
            ]),
            Transformation(name="LKP_REF", type="Lookup Procedure", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT"),
                TransformField(name="REF_VAL", datatype="string", port_type="LOOKUP/OUTPUT"),
            ], attributes={"Lookup Sql Override": "SELECT ID, REF_VAL FROM REF_TABLE"}),
            Transformation(name="RTR_STATUS", type="Router", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT", group="INPUT"),
                TransformField(name="STATUS", datatype="string", port_type="INPUT/OUTPUT", group="INPUT"),
                TransformField(name="ID1", datatype="number", port_type="OUTPUT", group="ACTIVE", expression="TRUE"),
                TransformField(name="ID1", datatype="number", port_type="OUTPUT", group="DEFAULT1"),
            ], groups=["INPUT", "ACTIVE", "DEFAULT1"]),
            Transformation(name="UPD_STRATEGY", type="Update Strategy", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT"),
                TransformField(name="STATUS", datatype="string", port_type="INPUT/OUTPUT"),
                TransformField(name="UPDATE_STRAT", datatype="number", port_type="OUTPUT",
                               expression="IIF(STATUS='NEW', DD_INSERT, DD_UPDATE)"),
            ]),
            Transformation(name="EXP_CALC", type="Expression", fields=[
                TransformField(name="ID", datatype="number", port_type="INPUT/OUTPUT"),
                TransformField(name="STATUS", datatype="string", port_type="INPUT/OUTPUT"),
                TransformField(name="PROCESSED", datatype="string", port_type="OUTPUT", expression="'Y'"),
            ]),
        ],
        instances=[
            Instance(name="SRC_A", transformation_name="SRC_A", transformation_type="Source Definition", instance_type="SOURCE"),
            Instance(name="SRC_B", transformation_name="SRC_B", transformation_type="Source Definition", instance_type="SOURCE"),
            Instance(name="SQ_A", transformation_name="SQ_A", transformation_type="Source Qualifier"),
            Instance(name="SQ_B", transformation_name="SQ_B", transformation_type="Source Qualifier"),
            Instance(name="JNR_AB", transformation_name="JNR_AB", transformation_type="Joiner"),
            Instance(name="LKP_REF", transformation_name="LKP_REF", transformation_type="Lookup Procedure"),
            Instance(name="RTR_STATUS", transformation_name="RTR_STATUS", transformation_type="Router"),
            Instance(name="UPD_STRATEGY", transformation_name="UPD_STRATEGY", transformation_type="Update Strategy"),
            Instance(name="EXP_CALC", transformation_name="EXP_CALC", transformation_type="Expression"),
            Instance(name="TGT_DIM", transformation_name="TGT_DIM", transformation_type="Target Definition", instance_type="TARGET"),
        ],
        connectors=[
            Connector(from_field="ID", from_instance="SRC_A", from_instance_type="Source Definition",
                      to_field="ID", to_instance="SQ_A", to_instance_type="Source Qualifier"),
            Connector(from_field="ID", from_instance="SQ_A", from_instance_type="Source Qualifier",
                      to_field="ID", to_instance="JNR_AB", to_instance_type="Joiner"),
            Connector(from_field="ID", from_instance="SRC_B", from_instance_type="Source Definition",
                      to_field="ID", to_instance="SQ_B", to_instance_type="Source Qualifier"),
            Connector(from_field="ID", from_instance="SQ_B", from_instance_type="Source Qualifier",
                      to_field="ID", to_instance="JNR_AB", to_instance_type="Joiner"),
            Connector(from_field="ID", from_instance="JNR_AB", from_instance_type="Joiner",
                      to_field="ID", to_instance="LKP_REF", to_instance_type="Lookup Procedure"),
            Connector(from_field="ID", from_instance="LKP_REF", from_instance_type="Lookup Procedure",
                      to_field="ID", to_instance="RTR_STATUS", to_instance_type="Router"),
            Connector(from_field="ID", from_instance="RTR_STATUS", from_instance_type="Router",
                      to_field="ID", to_instance="UPD_STRATEGY", to_instance_type="Update Strategy"),
            Connector(from_field="ID", from_instance="UPD_STRATEGY", from_instance_type="Update Strategy",
                      to_field="ID", to_instance="EXP_CALC", to_instance_type="Expression"),
            Connector(from_field="ID", from_instance="EXP_CALC", from_instance_type="Expression",
                      to_field="ID", to_instance="TGT_DIM", to_instance_type="Target Definition"),
        ],
    )


@pytest.fixture
def simple_repository(simple_mapping):
    """A repository with one folder containing the simple mapping."""
    folder = Folder(
        name="TestFolder",
        sources=simple_mapping.sources[:],
        targets=simple_mapping.targets[:],
        mappings=[simple_mapping],
    )
    return Repository(
        name="TEST_REPO",
        database_type="Oracle",
        folders=[folder],
    )


@pytest.fixture
def clean_dbt_files():
    """A clean set of generated dbt files with no issues."""
    return [
        GeneratedFile("models/staging/stg_orders.sql", (
            "{{ config(materialized='view') }}\n\n"
            "select\n"
            "    order_id,\n"
            "    cust_id,\n"
            "    amount\n"
            "from {{ source('oracle_raw', 'src_orders') }}\n"
        )),
        GeneratedFile("models/intermediate/int_orders_enriched.sql", (
            "{{ config(materialized='table') }}\n\n"
            "select\n"
            "    order_id,\n"
            "    cust_id,\n"
            "    amount,\n"
            "    amount * 1.1 as amount_usd\n"
            "from {{ ref('stg_orders') }}\n"
        )),
        GeneratedFile("models/marts/fct_orders.sql", (
            "{{ config(materialized='table') }}\n\n"
            "select *\n"
            "from {{ ref('int_orders_enriched') }}\n"
        )),
        GeneratedFile("models/staging/sources.yml", (
            "version: 2\n"
            "sources:\n"
            "  - name: oracle_raw\n"
            "    tables:\n"
            "      - name: src_orders\n"
        )),
        GeneratedFile("models/staging/schema.yml", (
            "version: 2\n"
            "models:\n"
            "  - name: stg_orders\n"
            "    columns:\n"
            "      - name: order_id\n"
            "        tests:\n"
            "          - not_null\n"
            "  - name: int_orders_enriched\n"
            "  - name: fct_orders\n"
        )),
    ]


@pytest.fixture
def problematic_dbt_files():
    """Generated dbt files with various issues for testing validators."""
    return [
        GeneratedFile("models/staging/stg_bad.sql", (
            "select IIF(status='A', 1, 0) as flag\n"
            "from PROD.APP.ORDERS;\n"
        )),
        GeneratedFile("models/staging/stg_dup.sql", (
            "{{ config(materialized='view') }}\n"
            "select * from {{ source('raw', 'tbl') }}\n"
        )),
        GeneratedFile("models/intermediate/stg_dup.sql", (
            "{{ config(materialized='table') }}\n"
            "select * from {{ ref('stg_bad') }}\n"
        )),
        GeneratedFile("models/staging/fct_wrong_layer.sql", (
            "{{ config(materialized='view') }}\n"
            "select * from {{ ref('stg_bad') }}\n"
        )),
        GeneratedFile("models/staging/schema.yml", (
            "version: 2\n"
            "models:\n"
            "  - name: stg_bad\n"
            "  - name: stg_phantom\n"
        )),
        GeneratedFile("models/staging/sources.yml", (
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    tables:\n"
            "      - name: tbl\n"
        )),
    ]
