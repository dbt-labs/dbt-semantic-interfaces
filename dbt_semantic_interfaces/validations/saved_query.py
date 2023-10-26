import logging
import traceback
from typing import Generic, List, Sequence, Set

from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.parsing.where_filter.where_filter_parser import (
    WhereFilterParser,
)
from dbt_semantic_interfaces.protocols import SemanticManifestT
from dbt_semantic_interfaces.protocols.saved_query import SavedQuery
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SavedQueryContext,
    SavedQueryElementType,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    generate_exception_issue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class SavedQueryRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates fields in a saved query.

    As the semantic model graph is not traversed completely in DSI, the validations for saved queries can't be complete.
    Consequently, the current plan is that we add a separate validation using MetricFlow in CI.

    * Check if metric names exist in the manifest.
    * Check that the where filter is valid using the same logic as WhereFiltersAreParsable
    """

    @staticmethod
    @validate_safely("Validate the group-by field in a saved query.")
    def _check_group_bys(valid_group_by_element_names: Set[str], saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for group_by_item in saved_query.query_params.group_by:
            # TODO: Replace with more appropriate abstractions once available.
            parameter_sets: FilterCallParameterSets
            try:
                parameter_sets = WhereFilterParser.parse_call_parameter_sets("{{" + group_by_item + "}}")
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse a group-by in saved query `{saved_query.name}`",
                        e=e,
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.WHERE,
                            element_value=group_by_item,
                        ),
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )
                continue

            element_names_in_group_by = (
                [x.entity_reference.element_name for x in parameter_sets.entity_call_parameter_sets]
                + [x.dimension_reference.element_name for x in parameter_sets.dimension_call_parameter_sets]
                + [x.time_dimension_reference.element_name for x in parameter_sets.time_dimension_call_parameter_sets]
            )

            if len(element_names_in_group_by) != 1 or element_names_in_group_by[0] not in valid_group_by_element_names:
                issues.append(
                    ValidationError(
                        message=f"`{group_by_item}` is not a valid group-by name.",
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.GROUP_BY,
                            element_value=group_by_item,
                        ),
                    )
                )
        return issues

    @staticmethod
    @validate_safely("Validate the metrics field in a saved query.")
    def _check_metrics(valid_metric_names: Set[str], saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        for metric_name in saved_query.query_params.metrics:
            if metric_name not in valid_metric_names:
                issues.append(
                    ValidationError(
                        message=f"`{metric_name}` is not a valid metric name.",
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.METRIC,
                            element_value=metric_name,
                        ),
                    )
                )
        return issues

    @staticmethod
    @validate_safely("Validate the where field in a saved query.")
    def _check_where(saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if saved_query.query_params.where is None:
            return issues
        for where_filter in saved_query.query_params.where.where_filters:
            try:
                where_filter.call_parameter_sets
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse a filter in saved query `{saved_query.name}`",
                        e=e,
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.WHERE,
                            element_value=where_filter.where_sql_template,
                        ),
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )

        return issues

    @staticmethod
    @validate_safely("Validate all saved queries in a semantic manifest.")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        valid_metric_names = {metric.name for metric in semantic_manifest.metrics}
        valid_group_by_element_names = {METRIC_TIME_ELEMENT_NAME}
        for semantic_model in semantic_manifest.semantic_models:
            for dimension in semantic_model.dimensions:
                valid_group_by_element_names.add(dimension.name)
            for entity in semantic_model.entities:
                valid_group_by_element_names.add(entity.name)

        for saved_query in semantic_manifest.saved_queries:
            issues += SavedQueryRule._check_metrics(
                valid_metric_names=valid_metric_names,
                saved_query=saved_query,
            )
            issues += SavedQueryRule._check_group_bys(
                valid_group_by_element_names=valid_group_by_element_names,
                saved_query=saved_query,
            )
            issues += SavedQueryRule._check_where(saved_query=saved_query)

        return issues
