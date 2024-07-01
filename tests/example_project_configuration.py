import textwrap

from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.time_spine import PydanticTimeSpine
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.type_enums import TimeGranularity

EXAMPLE_PROJECT_CONFIGURATION = PydanticProjectConfiguration(
    time_spines=[
        PydanticTimeSpine(
            name="example_time_spine",
            node_relation=PydanticNodeRelation(schema_name="example_schema", alias="example_table"),
            base_column="ds",
            base_granularity=TimeGranularity.DAY,
        )
    ],
)

EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE = YamlConfigFile(
    filepath="projection_configuration_yaml_file_path",
    contents=textwrap.dedent(
        """\
        project_configuration:
          time_spines:
            - name: sample
              node_relation:
                schema_name: sample
                alias: sample
              base_column: ds
              base_granularity: day
        """
    ),
)
