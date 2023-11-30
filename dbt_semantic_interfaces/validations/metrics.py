import traceback
from typing import Generic, List, Optional, Sequence

from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.protocols import (
    ConversionTypeParams,
    Metric,
    SemanticManifest,
    SemanticManifestT,
    SemanticModel,
)
from dbt_semantic_interfaces.references import MeasureReference, MetricModelReference
from dbt_semantic_interfaces.type_enums import AggregationType, MetricType
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    generate_exception_issue,
    validate_safely,
)


class CumulativeMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that cumulative sum metrics are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a cumulative sum metric")
    def _validate_cumulative_sum_metric_params(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.CUMULATIVE:
            if metric.type_params.window and metric.type_params.grain_to_date:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other",
                    )
                )

            if metric.type_params.window:
                try:
                    window_str = f"{metric.type_params.window.count} {metric.type_params.window.granularity.value}"
                    # TODO: Should not call an implementation class.
                    PydanticMetricTimeWindow.parse(window_str)
                except ParsingException as e:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message="".join(traceback.format_exception_only(type(e), value=e)),
                            extra_detail="".join(traceback.format_tb(e.__traceback__)),
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring cumulative sum metrics are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues


class DerivedMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that derived metrics are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the alias set are not unique and distinct")
    def _validate_alias_collision(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.DERIVED:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )
            used_names = {input_metric.name for input_metric in metric.input_metrics}
            for input_metric in metric.input_metrics:
                if input_metric.alias:
                    issues += UniqueAndValidNameRule.check_valid_name(input_metric.alias, metric_context)
                    if input_metric.alias in used_names:
                        issues.append(
                            ValidationError(
                                context=metric_context,
                                message=f"Alias '{input_metric.alias}' for input metric: '{input_metric.name}' is "
                                "already being used. Please choose another alias.",
                            )
                        )
                        used_names.add(input_metric.alias)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the input metrics exist")
    def _validate_input_metrics_exist(semantic_manifest: SemanticManifest) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        all_metrics = {m.name for m in semantic_manifest.metrics}
        for metric in semantic_manifest.metrics:
            if metric.type == MetricType.DERIVED:
                for input_metric in metric.input_metrics:
                    if input_metric.name not in all_metrics:
                        issues.append(
                            ValidationError(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not "
                                "exist as a configured metric in the model.",
                            )
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that input metric time offset params are valid")
    def _validate_time_offset_params(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for input_metric in metric.input_metrics or []:
            if input_metric.offset_window and input_metric.offset_to_grain:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"Both offset_window and offset_to_grain set for derived metric '{metric.name}' on "
                        f"input metric '{input_metric.name}'. Please set one or the other.",
                    )
                )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring derived metrics properties are configured properly"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        issues += DerivedMetricRule._validate_input_metrics_exist(semantic_manifest=semantic_manifest)
        for metric in semantic_manifest.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(metric=metric)
        return issues


class WhereFiltersAreParseable(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates that all Metric WhereFilters are parseable."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring a metric's filter properties are configured properly"
    )
    def _validate_metric(metric: Metric) -> Sequence[ValidationIssue]:  # noqa: D
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
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring all metric where filters are parseable")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            issues += WhereFiltersAreParseable._validate_metric(metric)
        return issues


class ConversionMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that conversion metrics are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a conversion metric")
    def _validate_type_params(metric: Metric, conversion_type_params: ConversionTypeParams) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        window = conversion_type_params.window
        if window:
            try:
                window_str = f"{window.count} {window.granularity.value}"
                PydanticMetricTimeWindow.parse(window_str)
            except ParsingException as e:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message="".join(traceback.format_exception_only(type(e), value=e)),
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the entity exists in the base/conversion semantic model")
    def _validate_entity_exists(
        metric: Metric, entity: str, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if entity not in {entity.name for entity in base_semantic_model.entities}:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Entity: {entity} not found in base semantic model: {base_semantic_model.name}.",
                )
            )
        if entity not in {entity.name for entity in conversion_semantic_model.entities}:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Entity: {entity} not found in "
                    f"conversion semantic model: {conversion_semantic_model.name}.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the provided measures are valid for conversion metrics")
    def _validate_measures(
        metric: Metric, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        def _validate_measure(measure_reference: MeasureReference, semantic_model: SemanticModel) -> None:
            measure = None
            for model_measure in semantic_model.measures:
                if model_measure.reference == measure_reference:
                    measure = model_measure
                    break

            assert measure, f"Measure '{model_measure.name}' wasn't found in semantic model '{semantic_model.name}'"

            if (
                measure.agg != AggregationType.COUNT
                and measure.agg != AggregationType.COUNT_DISTINCT
                and (measure.agg != AggregationType.SUM or measure.expr != "1")
            ):
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"For conversion metrics, the measure must be COUNT/SUM(1)/COUNT_DISTINCT. "
                        f"Measure: {measure.name} is agg type: {measure.agg}",
                    )
                )

        conversion_type_params = metric.type_params.conversion_type_params
        assert (
            conversion_type_params is not None
        ), "For a conversion metric, type_params.conversion_type_params must exist."
        _validate_measure(
            measure_reference=conversion_type_params.base_measure.measure_reference,
            semantic_model=base_semantic_model,
        )
        _validate_measure(
            measure_reference=conversion_type_params.conversion_measure.measure_reference,
            semantic_model=conversion_semantic_model,
        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the provided constant properties are valid")
    def _validate_constant_properties(
        metric: Metric, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        def _elements_in_model(references: List[str], semantic_model: SemanticModel) -> None:
            linkable_elements = [entity.name for entity in semantic_model.entities] + [
                dimension.name for dimension in semantic_model.dimensions
            ]
            for reference in references:
                if reference not in linkable_elements:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"The provided constant property: {reference}, "
                            f"cannot be found in semantic model {semantic_model.name}",
                        )
                    )

        conversion_type_params = metric.type_params.conversion_type_params
        assert (
            conversion_type_params is not None
        ), "For a conversion metric, type_params.conversion_type_params must exist."
        constant_properties = conversion_type_params.constant_properties or []
        base_properties = []
        conversion_properties = []
        for constant_property in constant_properties:
            base_properties.append(constant_property.base_property)
            conversion_properties.append(constant_property.conversion_property)

        _elements_in_model(references=base_properties, semantic_model=base_semantic_model)
        _elements_in_model(references=conversion_properties, semantic_model=conversion_semantic_model)
        return issues

    @staticmethod
    def _get_semantic_model_from_measure(
        measure_reference: MeasureReference, semantic_manifest: SemanticManifest
    ) -> Optional[SemanticModel]:
        """Retrieve the semantic model from a given measure reference."""
        semantic_model = None
        for model in semantic_manifest.semantic_models:
            if measure_reference in {measure.reference for measure in model.measures}:
                semantic_model = model
                break
        return semantic_model

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring conversion metrics are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            if metric.type == MetricType.CONVERSION:
                # Validates that the measure exists and corresponds to a semantic model
                assert (
                    metric.type_params.conversion_type_params is not None
                ), "For a conversion metric, type_params.conversion_type_params must exist."

                base_semantic_model = ConversionMetricRule._get_semantic_model_from_measure(
                    measure_reference=metric.type_params.conversion_type_params.base_measure.measure_reference,
                    semantic_manifest=semantic_manifest,
                )
                conversion_semantic_model = ConversionMetricRule._get_semantic_model_from_measure(
                    measure_reference=metric.type_params.conversion_type_params.conversion_measure.measure_reference,
                    semantic_manifest=semantic_manifest,
                )
                if base_semantic_model is None or conversion_semantic_model is None:
                    # If measure's don't exist, stop this metric's validation as it will fail later validations
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"For metric '{metric.name}', conversion measures specified was not found.",
                        )
                    )
                    continue

                issues += ConversionMetricRule._validate_entity_exists(
                    metric=metric,
                    entity=metric.type_params.conversion_type_params.entity,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
                issues += ConversionMetricRule._validate_measures(
                    metric=metric,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
                issues += ConversionMetricRule._validate_type_params(
                    metric=metric, conversion_type_params=metric.type_params.conversion_type_params
                )
                issues += ConversionMetricRule._validate_constant_properties(
                    metric=metric,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
        return issues
