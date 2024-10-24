import copy
import logging

from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.saved_query import (
    PydanticSavedQuery,
    PydanticSavedQueryQueryParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.validations.saved_query import SavedQueryRule
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationResults,
)

logger = logging.getLogger(__name__)


def check_only_one_error_with_message(  # noqa: D
    results: SemanticManifestValidationResults, target_message: str
) -> None:
    assert len(results.warnings) == 0
    assert len(results.errors) == 1
    assert len(results.future_errors) == 0

    found_match = results.errors[0].message.find(target_message) != -1
    # Adding this dict to the assert so that when it does not match, pytest prints the expected and actual values.
    assert {
        "expected": target_message,
        "actual": results.errors[0].message,
    } and found_match


def check_only_one_warning_with_message(  # noqa: D
    results: SemanticManifestValidationResults, target_message: str
) -> None:
    assert len(results.warnings) == 1
    assert len(results.errors) == 0
    assert len(results.future_errors) == 0

    found_match = results.warnings[0].message.find(target_message) != -1
    # Adding this dict to the assert so that when it does not match, pytest prints the expected and actual values.
    assert {
        "expected": target_message,
        "actual": results.warnings[0].message,
    } and found_match


def test_invalid_metric_in_saved_query(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["invalid_metric"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest), "is not a valid metric name."
    )


def test_invalid_where_in_saved_query(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ invalid_jinja }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "trying to parse a filter in saved query",
    )


def test_where_filter_validations_invalid_granularity(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[
                        PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'cool') }}"),
                    ]
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_warning_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "is not a valid granularity name",
    )


def test_invalid_group_by_element_in_saved_query(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__invalid_dimension')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "is not a valid group-by name.",
    )


def test_invalid_group_by_format_in_saved_query(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["invalid_format"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while trying to parse a group-by in saved query",
    )


def test_metric_filter_error(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Metric('bookings') }} > 2")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while trying to parse a filter in saved query",
    )


def test_metric_filter_success(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Metric('bookings', ['listing']) }} > 2")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    results = manifest_validator.validate_semantic_manifest(manifest)
    assert len(results.warnings) == 0
    assert len(results.errors) == 0
    assert len(results.future_errors) == 0
