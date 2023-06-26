import textwrap

from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
)
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)
from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_legacy_measure_metric_parsing() -> None:
    """Test for parsing a simple metric specification with the `measure` parameter instead of `measures`."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: legacy_test
          type: simple
          type_params:
            measure: legacy_measure
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "legacy_test"
    assert metric.type is MetricType.SIMPLE
    assert metric.simple_metric_parameters.measure == PydanticMetricInputMeasure(name="legacy_measure")


def test_legacy_metric_input_measure_object_parsing() -> None:
    """Test for parsing a simple metric specification with the `measure` parameter set with object notation."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: legacy_test
          type: simple
          type_params:
            measure:
              name: legacy_measure_from_object
              filter: "{{ dimension('some_bool') }}"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.simple_metric_parameters.measure == PydanticMetricInputMeasure(
        name="legacy_measure_from_object",
        filter=PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}"""),
    )


def test_metric_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the PydanticMetric object."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: metadata_test
          type: simple
          type_params:
            measure:
              name: metadata_test_measure
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.metadata is not None
    assert metric.metadata.repo_file_path == "test_dir/inline_for_test"
    assert metric.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        type: simple
        type_params:
          measure:
            name: metadata_test_measure
        """
    )
    assert metric.metadata.file_slice.content == expected_metadata_content


def test_ratio_metric_parsing() -> None:
    """Test for parsing a ratio metric specification with numerator and denominator."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: ratio_test
          type: ratio
          type_params:
            numerator:
              name: numerator_metric
            denominator:
              name: denominator_metric
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "ratio_test"
    assert metric.type is MetricType.RATIO
    assert metric.ratio_metric_parameters.numerator == PydanticMetricInput(name="numerator_metric")
    assert metric.ratio_metric_parameters.denominator == PydanticMetricInput(name="denominator_metric")


def test_ratio_metric_input_measure_object_parsing() -> None:
    """Test for parsing a ratio metric specification with object inputs for numerator and denominator."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: ratio_test
          type: ratio
          type_params:
            numerator:
              name: numerator_metric_from_object
              filter: "some_number > 5"
            denominator:
              name: denominator_metric_from_object
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.ratio_metric_parameters.numerator == PydanticMetricInput(
        name="numerator_metric_from_object",
        filter=PydanticWhereFilter(
            where_sql_template="some_number > 5",
        ),
    )
    assert metric.ratio_metric_parameters.denominator == PydanticMetricInput(name="denominator_metric_from_object")


def test_cumulative_window_metric_parsing() -> None:
    """Test for parsing a metric specification with a cumulative window."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: cumulative_test
          type: cumulative
          type_params:
            measure:
              name: cumulative_measure
            window: "7 days"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "cumulative_test"
    assert metric.type is MetricType.CUMULATIVE
    assert metric.cumulative_metric_parameters.measure == PydanticMetricInputMeasure(name="cumulative_measure")
    assert metric.cumulative_metric_parameters.window == PydanticMetricTimeWindow(
        count=7, granularity=TimeGranularity.DAY
    )


def test_grain_to_date_metric_parsing() -> None:
    """Test for parsing a metric specification with the grain to date cumulative setting."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: grain_to_date_test
          type: cumulative
          type_params:
            measure:
              name: cumulative_measure
            grain_to_date: "week"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "grain_to_date_test"
    assert metric.type is MetricType.CUMULATIVE
    assert metric.cumulative_metric_parameters.measure == PydanticMetricInputMeasure(name="cumulative_measure")
    assert metric.cumulative_metric_parameters.window is None
    assert metric.cumulative_metric_parameters.grain_to_date is TimeGranularity.WEEK


def test_derived_metric_offset_window_parsing() -> None:
    """Test for parsing a derived metric with an offset window."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: derived_offset_test
          type: derived
          type_params:
            expr: bookings / bookings_2_weeks_ago
            metrics:
              - name: bookings
              - name: bookings
                offset_window: 14 days
                alias: bookings_2_weeks_ago
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.issues.all_issues) == 0
    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "derived_offset_test"
    assert metric.type is MetricType.DERIVED
    assert metric.derived_metric_parameters.metrics and len(metric.derived_metric_parameters.metrics) == 2
    metric1, metric2 = metric.derived_metric_parameters.metrics
    assert metric1.offset_window is None
    assert metric2.offset_window == PydanticMetricTimeWindow(count=14, granularity=TimeGranularity.DAY)
    assert metric1.alias is None
    assert metric2.alias == "bookings_2_weeks_ago"
    assert metric.derived_metric_parameters.expr == "bookings / bookings_2_weeks_ago"


def test_derive_metric_offset_to_grain_parsing() -> None:
    """Test for parsing a derived metric with an offset to grain to date."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: derived_offset_to_grain_test
          type: derived
          type_params:
            expr: bookings / bookings_at_start_of_month
            metrics:
              - name: bookings
              - name: bookings
                offset_to_grain: month
                alias: bookings_at_start_of_month
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.issues.all_issues) == 0
    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "derived_offset_to_grain_test"
    assert metric.type is MetricType.DERIVED
    assert metric.derived_metric_parameters.metrics and len(metric.derived_metric_parameters.metrics) == 2
    metric1, metric2 = metric.derived_metric_parameters.metrics
    assert metric1.offset_to_grain is None
    assert metric2.offset_to_grain == TimeGranularity.MONTH
    assert metric1.alias is None
    assert metric2.alias == "bookings_at_start_of_month"
    assert metric.derived_metric_parameters.expr == "bookings / bookings_at_start_of_month"


def test_constraint_metric_parsing() -> None:
    """Test for parsing a metric specification with a constraint included."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: constraint_test
          type: simple
          type_params:
            measure:
              name: input_measure
          filter: "{{ dimension('some_dimension') }} IN ('value1', 'value2')"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "constraint_test"
    assert metric.type is MetricType.SIMPLE
    assert metric.filter == PydanticWhereFilter(
        where_sql_template="{{ dimension('some_dimension') }} IN ('value1', 'value2')"
    )


def test_derived_metric_input_parsing() -> None:
    """Test for parsing derived metrics with metric_input properties."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: derived_metric_test
          type: derived
          type_params:
            expr: sum(constrained_input_metric) / input_metric
            metrics:
              - name: input_metric
              - name: input_metric
                alias: constrained_input_metric
                filter: input_metric < 10
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.metrics) == 1
    metric = build_result.semantic_manifest.metrics[0]
    assert metric.name == "derived_metric_test"
    assert metric.type is MetricType.DERIVED
    assert metric.derived_metric_parameters.metrics
    assert len(metric.derived_metric_parameters.metrics) == 2
    assert metric.derived_metric_parameters.metrics[0] == PydanticMetricInput(name="input_metric")
    assert metric.derived_metric_parameters.metrics[1] == PydanticMetricInput(
        name="input_metric",
        alias="constrained_input_metric",
        filter=PydanticWhereFilter(where_sql_template="input_metric < 10"),
    )


def test_invalid_metric_type_parsing_error() -> None:
    """Test for error detection when parsing a metric specification with an invalid MetricType input value."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_type_test
          type: this is not a valid type
          type_params:
            measure:
              name: input_measure
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert build_result.issues.has_blocking_issues
    assert "'this is not a valid type' is not one of" in str(
        SemanticManifestValidationException(build_result.issues.all_issues)
    )


def test_invalid_cumulative_metric_window_format_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_format_test
          type: cumulative
          type_params:
            measure:
              name: cumulative_measure
            window: "7 days long"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert build_result.issues.has_blocking_issues
    assert "Invalid window" in str(SemanticManifestValidationException(build_result.issues.all_issues))


def test_invalid_cumulative_metric_window_granularity_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_granularity_test
          type: cumulative
          type_params:
            measure:
              name: cumulative_measure
            window: "7 moons"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert build_result.issues.has_blocking_issues
    assert "Invalid time granularity" in str(SemanticManifestValidationException(build_result.issues.all_issues))


def test_invalid_cumulative_metric_window_count_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries."""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_count_test
          type: cumulative
          type_params:
            measure:
              name: cumulative_measure
            window: "six days"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert build_result.issues.has_blocking_issues
    assert "Invalid count" in str(SemanticManifestValidationException(build_result.issues.all_issues))
