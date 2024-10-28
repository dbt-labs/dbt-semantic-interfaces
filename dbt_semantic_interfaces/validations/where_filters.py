import traceback
from typing import Generic, List, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.protocols import Metric, SemanticManifestT
from dbt_semantic_interfaces.protocols.saved_query import SavedQuery
from dbt_semantic_interfaces.references import MetricModelReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SavedQueryContext,
    SavedQueryElementType,
    SemanticManifestValidationRule,
    ValidationContext,
    ValidationIssue,
    ValidationWarning,
    generate_exception_issue,
    validate_safely,
)


class WhereFiltersAreParseableRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates that all Metric WhereFilters are parseable."""

    @staticmethod
    def _validate_time_granularity_names_impl(
        location_label_for_errors: str,
        filter_call_parameter_sets_with_context: Sequence[Tuple[ValidationContext, FilterCallParameterSets]],
        custom_granularity_names: List[str],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        valid_granularity_names = [
            standard_granularity.value for standard_granularity in TimeGranularity
        ] + custom_granularity_names

        for context, parameter_set in filter_call_parameter_sets_with_context:
            for time_dim_call_parameter_set in parameter_set.time_dimension_call_parameter_sets:
                if not time_dim_call_parameter_set.time_granularity_name:
                    continue
                if time_dim_call_parameter_set.time_granularity_name.lower() not in valid_granularity_names:
                    issues.append(
                        ValidationWarning(
                            context=context,
                            message=f"Filter for `{location_label_for_errors}` is not valid. "
                            f"`{time_dim_call_parameter_set.time_granularity_name}` is not a valid granularity name. "
                            f"Valid granularity options: {valid_granularity_names}",
                        )
                    )
        return issues

    @staticmethod
    def _validate_time_granularity_names_for_saved_query(
        saved_query: SavedQuery, custom_granularity_names: List[str]
    ) -> Sequence[ValidationIssue]:
        where_param = saved_query.query_params.where
        if where_param is None:
            return []

        return WhereFiltersAreParseableRule._validate_time_granularity_names_impl(
            location_label_for_errors="saved query `{saved_query.name}`",
            filter_call_parameter_sets_with_context=[
                (
                    SavedQueryContext(
                        file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                        element_type=SavedQueryElementType.WHERE,
                        element_value=where_filter.where_sql_template,
                    ),
                    where_filter.call_parameter_sets,
                )
                for where_filter in where_param.where_filters
            ],
            custom_granularity_names=custom_granularity_names,
        )

    @staticmethod
    def _validate_time_granularity_names_for_metric(
        context: MetricContext,
        filter_expression_parameter_sets: Sequence[Tuple[str, FilterCallParameterSets]],
        custom_granularity_names: List[str],
    ) -> Sequence[ValidationIssue]:
        return WhereFiltersAreParseableRule._validate_time_granularity_names_impl(
            location_label_for_errors="metric `{context.metric.metric_name}`",
            filter_call_parameter_sets_with_context=[
                (
                    context,
                    param_set[1],
                )
                for param_set in filter_expression_parameter_sets
            ],
            custom_granularity_names=custom_granularity_names,
        )

    @staticmethod
    @validate_safely("validating the where field in a saved query.")
    def _validate_saved_query(
        saved_query: SavedQuery, custom_granularity_names: List[str]
    ) -> Sequence[ValidationIssue]:
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
            else:
                issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_saved_query(
                    saved_query, custom_granularity_names
                )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring a metric's filter properties are configured properly"
    )
    def _validate_metric(metric: Metric, custom_granularity_names: List[str]) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        context = MetricContext(
            file_context=FileContext.from_metadata(metadata=metric.metadata),
            metric=MetricModelReference(metric_name=metric.name),
        )

        if metric.filter is not None:
            try:
                metric.filter.filter_expression_parameter_sets
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse filter of metric `{metric.name}`",
                        e=e,
                        context=context,
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )
            else:
                issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_metric(
                    context=context,
                    filter_expression_parameter_sets=metric.filter.filter_expression_parameter_sets,
                    custom_granularity_names=custom_granularity_names,
                )

        if metric.type_params:
            measure = metric.type_params.measure
            if measure is not None and measure.filter is not None:
                try:
                    measure.filter.filter_expression_parameter_sets
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse filter of measure input `{measure.name}` "
                            f"on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=measure.filter.filter_expression_parameter_sets,
                        custom_granularity_names=custom_granularity_names,
                    )

            numerator = metric.type_params.numerator
            if numerator is not None and numerator.filter is not None:
                try:
                    numerator.filter.filter_expression_parameter_sets
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse the numerator filter on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=numerator.filter.filter_expression_parameter_sets,
                        custom_granularity_names=custom_granularity_names,
                    )

            denominator = metric.type_params.denominator
            if denominator is not None and denominator.filter is not None:
                try:
                    denominator.filter.filter_expression_parameter_sets
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse the denominator filter on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=denominator.filter.filter_expression_parameter_sets,
                        custom_granularity_names=custom_granularity_names,
                    )

            for input_metric in metric.type_params.metrics or []:
                if input_metric.filter is not None:
                    try:
                        input_metric.filter.filter_expression_parameter_sets
                    except Exception as e:
                        issues.append(
                            generate_exception_issue(
                                what_was_being_done=f"trying to parse filter for input metric `{input_metric.name}` "
                                f"on metric `{metric.name}`",
                                e=e,
                                context=context,
                                extras={
                                    "traceback": "".join(traceback.format_tb(e.__traceback__)),
                                },
                            )
                        )
                    else:
                        issues += WhereFiltersAreParseableRule._validate_time_granularity_names_for_metric(
                            context=context,
                            filter_expression_parameter_sets=input_metric.filter.filter_expression_parameter_sets,
                            custom_granularity_names=custom_granularity_names,
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring all metric where filters are parseable")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        custom_granularity_names = [
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        ]
        for metric in semantic_manifest.metrics or []:
            issues += WhereFiltersAreParseableRule._validate_metric(
                metric=metric, custom_granularity_names=custom_granularity_names
            )
        for saved_query in semantic_manifest.saved_queries:
            issues += WhereFiltersAreParseableRule._validate_saved_query(saved_query, custom_granularity_names)

        return issues
