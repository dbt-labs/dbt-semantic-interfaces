import textwrap

from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.time_spine import (
    PydanticTimeSpine,
    PydanticTimeSpinePrimaryColumn,
)
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.type_enums import TimeGranularity

EXAMPLE_PROJECT_CONFIGURATION = PydanticProjectConfiguration(
    time_spine_table_configurations=[
        PydanticTimeSpineTableConfiguration(
            location="example_schema.example_table",
            column_name="ds",
            grain=TimeGranularity.DAY,
        )
    ],
    time_spines=[
        PydanticTimeSpine(
            name="day_time_spine",
            node_relation=PydanticNodeRelation(alias="day_time_spine", schema_name="stuff"),
            primary_column=PydanticTimeSpinePrimaryColumn(name="ds_day", time_granularity=TimeGranularity.DAY),
        )
    ],
)

EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE = YamlConfigFile(
    filepath="projection_configuration_yaml_file_path",
    contents=textwrap.dedent(
        """\
        project_configuration:
          time_spine_table_configurations:
            - location: example_schema.example_table
              column_name: ds
              grain: day
          time_spines:
            - name: day_time_spine
              node_relation:
                schema_name: stuff
                alias: day_time_spine
              primary_column:
                name: ds_day
                time_granularity: day
        """
    ),
)
