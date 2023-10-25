import textwrap

from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)
from dbt_semantic_interfaces.type_enums.export_destination_type import ExportDestinationType


def test_saved_query_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the SavedQuery object."""
    yaml_contents = textwrap.dedent(
        """\
        saved_query:
            name: metadata_test
            metrics:
                - test_metric
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert saved_query.metadata is not None
    assert saved_query.metadata.repo_file_path == "test_dir/inline_for_test"
    assert saved_query.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        metrics:
        - test_metric
        """
    )
    assert saved_query.metadata.file_slice.content == expected_metadata_content


def test_saved_query_base_parsing() -> None:
    """Test for base parsing a saved query."""
    name = "base_parsing_test"
    description = "the base saved query parsing test"
    label = "Base Parsing Test"
    yaml_contents = textwrap.dedent(
        f"""\
        saved_query:
          name: {name}
          description: {description}
          label: {label}
          metrics:
            - test_metric
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert saved_query.name == name
    assert saved_query.description == description
    assert saved_query.label == label


def test_saved_query_metrics_parsing() -> None:
    """Test for parsing metrics referenced in a saved query."""
    yaml_contents = textwrap.dedent(
        """\
        saved_query:
          name: test_saved_query_metrics
          metrics:
            - test_metric_a
            - test_metric_b
            - test_metric_c
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert len(saved_query.metrics) == 3
    assert {"test_metric_a", "test_metric_b", "test_metric_c"} == set(saved_query.metrics)


def test_saved_query_group_bys() -> None:
    """Test for parsing group_bys in a saved query."""
    yaml_contents = textwrap.dedent(
        """\
        saved_query:
          name: test_saved_query_group_bys
          metrics:
            - test_metric_a
          group_bys:
            - Dimension(test_entity__test_dimension_a)
            - Dimension(test_entity__test_dimension_b)

        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert len(saved_query.group_bys) == 2
    assert {"Dimension(test_entity__test_dimension_a)", "Dimension(test_entity__test_dimension_b)"} == set(
        saved_query.group_bys
    )


def test_saved_query_where() -> None:
    """Test for parsing where clause in a saved query."""
    where = "Dimension(test_entity__test_dimension) == true"
    yaml_contents = textwrap.dedent(
        f"""\
        saved_query:
          name: test_saved_query_where_clause
          metrics:
            - test_metric_a
          where:
            - '{where}'

        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert saved_query.where is not None
    assert len(saved_query.where.where_filters) == 1
    assert where == saved_query.where.where_filters[0].where_sql_template


def test_saved_query_exports() -> None:
    """Test for parsing exports referenced in a saved query."""
    yaml_contents = textwrap.dedent(
        """\
        saved_query:
          name: test_exports
          metrics:
            - test_metric_a
          exports:
            - name: test_exports1
              config:
                export_as: VIEW
                schema: my_schema
                alias: my_view_name
            - name: test_exports2
              config:
                export_as: table
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.saved_queries) == 1
    saved_query = build_result.semantic_manifest.saved_queries[0]
    assert len(saved_query.exports) == 2
    names_to_exports = {export.name: export for export in saved_query.exports}
    assert set(names_to_exports.keys()) == {"test_exports1", "test_exports2"}

    export1_config = names_to_exports["test_exports1"].config
    assert export1_config.export_as == ExportDestinationType.VIEW
    assert export1_config.schema_name == "my_schema"
    assert export1_config.alias == "my_view_name"

    export2_config = names_to_exports["test_exports2"].config
    assert export2_config.export_as == ExportDestinationType.TABLE
    assert export2_config.schema_name is None
    assert export2_config.alias is None
