from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from dbt_semantic_interfaces.type_enums import AggregationType, MetricType
from dbt_semantic_interfaces.validations.measures import SemanticModelMeasuresUniqueRule
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION
from tests.validations.validation_test_utils import check_error_in_issues


def test_measure_and_simple_metric_same_name_warns() -> None:
    """If a metric shares a name with a measure and is SIMPLE, return a validation warning."""
    measure_name = "num_sample_rows"
    semantic_model_name = "sample_semantic_model"

    semantic_model = semantic_model_with_guaranteed_meta(
        name=semantic_model_name,
        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM, expr="1")],
    )
    semantic_model.measures[0].create_metric = True

    simple_metric = metric_with_guaranteed_meta(
        name=measure_name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[simple_metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    validation_results = SemanticManifestValidator[PydanticSemanticManifest](
        [SemanticModelMeasuresUniqueRule()]
    ).validate_semantic_manifest(manifest)

    expected_warning = (
        f"A measure with name '{measure_name}' exists in semantic model '{semantic_model_name}', and a simple metric"
    )
    check_error_in_issues(error_substrings=[expected_warning], issues=validation_results.warnings)


def test_measure_and_non_simple_metric_same_name_errors() -> None:
    """If a metric shares a name with a measure and is NOT SIMPLE, return a validation error."""
    measure_name = "num_sample_rows"
    semantic_model_name = "sample_semantic_model"

    semantic_model = semantic_model_with_guaranteed_meta(
        name=semantic_model_name,
        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM, expr="1")],
    )
    semantic_model.measures[0].create_metric = True

    non_simple_metric = metric_with_guaranteed_meta(
        name=measure_name,
        type=MetricType.DERIVED,
        type_params=PydanticMetricTypeParams(expr=measure_name),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[non_simple_metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    validation_results = SemanticManifestValidator[PydanticSemanticManifest](
        [SemanticModelMeasuresUniqueRule()]
    ).validate_semantic_manifest(manifest)

    expected_error = (
        f"Cannot auto-generate a proxy metric for measure '{measure_name}' in semantic "
        f"model '{semantic_model_name}' because a metric with the same name already exists "
        f"in the project. This is elevated to an error instead of a warning because the "
        f"metric is not a simple metric (found type: '{MetricType.DERIVED}'). Rename the "
        f"measure or the metric to resolve the collision."
    )
    check_error_in_issues(error_substrings=[expected_error], issues=validation_results.errors)
