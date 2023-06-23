from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity

EXAMPLE_PROJECT_CONFIGURATIONS = [
    PydanticProjectConfiguration(
        time_spine_table_configurations=[
            PydanticTimeSpineTableConfiguration(
                location="example_schema.example_table",
                column_name="ds",
                grain=TimeGranularity.DAY,
            )
        ],
    )
]
