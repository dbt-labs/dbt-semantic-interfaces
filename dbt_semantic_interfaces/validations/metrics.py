import traceback
from typing import Dict, Generic, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricTimeWindow,
)
from dbt_semantic_interfaces.protocols import (
    ConversionTypeParams,
    Dimension,
    Metric,
    MetricInputMeasure,
    SemanticManifest,
    SemanticManifestT,
    SemanticModel,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    MeasureReference,
    MetricModelReference,
    MetricReference,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    MetricType,
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
    generate_exception_issue,
    validate_safely,
)


class CumulativeMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that cumulative metrics are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring cumulative metrics are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            if metric.type != MetricType.CUMULATIVE:
                continue

            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )

            for field in ("window", "grain_to_date"):
                type_params_field_value = getattr(metric.type_params, field)

                # Warn that window or grain_to_date is mismatched across params.
                cumulative_type_params_field_value = (
                    getattr(metric.type_params.cumulative_type_params, field)
                    if metric.type_params.cumulative_type_params
                    else None
                )
                if (
                    type_params_field_value
                    and cumulative_type_params_field_value
                    and cumulative_type_params_field_value != type_params_field_value
                ):
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Got differing values for `{field}` on cumulative metric '{metric.name}'. In "
                                f"`type_params.{field}`, got '{type_params_field_value}'. In "
                                f"`type_params.cumulative_type_params.{field}`, got "
                                f"'{cumulative_type_params_field_value}'. Please remove the value from "
                                f"`type_params.{field}`."
                            ),
                        )
                    )

            window = metric.type_params.window
            if metric.type_params.cumulative_type_params and metric.type_params.cumulative_type_params.window:
                window = metric.type_params.cumulative_type_params.window
            grain_to_date = metric.type_params.grain_to_date
            if metric.type_params.cumulative_type_params and metric.type_params.cumulative_type_params.grain_to_date:
                grain_to_date = metric.type_params.cumulative_type_params.grain_to_date
            if window and grain_to_date:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other.",
                    )
                )

            if window:
                try:
                    window_str = f"{window.count} {window.granularity.value}"
                    # TODO: Should not call an implementation class.
                    PydanticMetricTimeWindow.parse(window_str)
                except ParsingException as e:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message="".join(traceback.format_exception_only(type(e), value=e)),
                            extra_detail="".join(traceback.format_tb(e.__traceback__)),
                        )
                    )

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
            input_metrics = metric.type_params.metrics or []
            used_names = {input_metric.name for input_metric in input_metrics}
            for input_metric in input_metrics:
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
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )
            if metric.type == MetricType.DERIVED:
                if not metric.type_params.metrics:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=f"No input metrics found for derived metric '{metric.name}'. "
                            "Please add metrics to type_params.metrics.",
                        )
                    )
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.name not in all_metrics:
                        issues.append(
                            ValidationError(
                                context=metric_context,
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not "
                                "exist as a configured metric in the model.",
                            )
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that input metric time offset params are valid")
    def _validate_time_offset_params(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for input_metric in metric.type_params.metrics or []:
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
    @validate_safely(whats_being_done="checking that the expr field uses the input metrics")
    def _validate_expr(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.DERIVED:
            if not metric.type_params.expr:
                issues.append(
                    ValidationWarning(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"No `expr` set for derived metric {metric.name}. "
                        "Please add an `expr` that references all input metrics.",
                    )
                )
            else:
                for input_metric in metric.type_params.metrics or []:
                    name = input_metric.alias or input_metric.name
                    if name not in metric.type_params.expr:
                        issues.append(
                            ValidationWarning(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"Input metric '{name}' is not used in `expr`: '{metric.type_params.expr}' for "
                                f"derived metric '{metric.name}'. Please update the `expr` or remove the input metric.",
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
            issues += DerivedMetricRule._validate_expr(metric=metric)
        return issues


class WhereFiltersAreParseable(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates that all Metric WhereFilters are parseable."""

    @staticmethod
    def _validate_time_granularity_names(
        context: MetricContext,
        filter_expression_parameter_sets: Sequence[Tuple[str, FilterCallParameterSets]],
        custom_granularity_names: List[str],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        valid_granularity_names = [
            standard_granularity.value for standard_granularity in TimeGranularity
        ] + custom_granularity_names
        for _, parameter_set in filter_expression_parameter_sets:
            for time_dim_call_parameter_set in parameter_set.time_dimension_call_parameter_sets:
                if not time_dim_call_parameter_set.time_granularity_name:
                    continue
                if time_dim_call_parameter_set.time_granularity_name.lower() not in valid_granularity_names:
                    issues.append(
                        ValidationWarning(
                            context=context,
                            message=f"Filter for metric `{context.metric.metric_name}` is not valid. "
                            f"`{time_dim_call_parameter_set.time_granularity_name}` is not a valid granularity name. "
                            f"Valid granularity options: {valid_granularity_names}",
                        )
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
                issues += WhereFiltersAreParseable._validate_time_granularity_names(
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
                    issues += WhereFiltersAreParseable._validate_time_granularity_names(
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
                    issues += WhereFiltersAreParseable._validate_time_granularity_names(
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
                    issues += WhereFiltersAreParseable._validate_time_granularity_names(
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
                        issues += WhereFiltersAreParseable._validate_time_granularity_names(
                            context=context,
                            filter_expression_parameter_sets=input_metric.filter.filter_expression_parameter_sets,
                            custom_granularity_names=custom_granularity_names,
                        )

            # TODO: Are saved query filters being validated? Task: SL-2932
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
            issues += WhereFiltersAreParseable._validate_metric(
                metric=metric, custom_granularity_names=custom_granularity_names
            )
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

        def _validate_measure(
            input_measure: MetricInputMeasure, semantic_model: SemanticModel, is_base_measure: bool = True
        ) -> None:
            measure = None
            for model_measure in semantic_model.measures:
                if model_measure.reference == input_measure.measure_reference:
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

            if input_measure.filter is not None and not is_base_measure:
                # Filters for conversion measure input are not fully supported.
                issues.append(
                    ValidationWarning(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"Measure input {measure.name} has a filter. For conversion metrics,"
                        " filtering on a conversion input measure is not fully supported yet.",
                    )
                )

        conversion_type_params = metric.type_params.conversion_type_params
        assert (
            conversion_type_params is not None
        ), "For a conversion metric, type_params.conversion_type_params must exist."
        _validate_measure(
            input_measure=conversion_type_params.base_measure,
            semantic_model=base_semantic_model,
        )
        _validate_measure(
            input_measure=conversion_type_params.conversion_measure,
            semantic_model=conversion_semantic_model,
            is_base_measure=False,
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


class MetricTimeGranularityRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that time_granularity set for metric is queryable for that metric."""

    @staticmethod
    def _min_queryable_granularity_for_metric(
        metric: Metric,
        metric_index: Dict[MetricReference, Metric],
        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]],
    ) -> Optional[TimeGranularity]:
        """Get the minimum time granularity this metric is allowed to be queried with.

        This should be the largest granularity that any of the metric's agg_time_dimensions is defined at.
        Defaults to DAY in the
        """
        min_queryable_granularity: Optional[TimeGranularity] = None
        for measure_reference in PydanticMetric.all_input_measures_for_metric(metric=metric, metric_index=metric_index):
            agg_time_dimension = measure_to_agg_time_dimension.get(measure_reference)
            if not agg_time_dimension:
                # This indicates the measure or agg_time_dimension were invalid, so we can't determine granularity.
                return None
            defined_time_granularity = (
                agg_time_dimension.type_params.time_granularity
                if agg_time_dimension.type_params
                else TimeGranularity.DAY
            )
            if not min_queryable_granularity or defined_time_granularity.to_int() > min_queryable_granularity.to_int():
                min_queryable_granularity = defined_time_granularity

        return min_queryable_granularity

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring a metric's time_granularity is valid for the metric"
    )
    def _validate_metric(
        metric: Metric,
        metric_index: Dict[MetricReference, Metric],
        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]],
    ) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        context = MetricContext(
            file_context=FileContext.from_metadata(metadata=metric.metadata),
            metric=MetricModelReference(metric_name=metric.name),
        )

        if metric.time_granularity:
            min_queryable_granularity = MetricTimeGranularityRule._min_queryable_granularity_for_metric(
                metric=metric, metric_index=metric_index, measure_to_agg_time_dimension=measure_to_agg_time_dimension
            )
            if not min_queryable_granularity:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"Unable to validate `time_granularity` for metric '{metric.name}' due to "
                            "misconfiguration with measures or related agg_time_dimensions."
                        ),
                    )
                )
                return issues
            valid_granularities = [
                granularity.name
                for granularity in TimeGranularity
                if granularity.to_int() >= min_queryable_granularity.to_int()
            ]
            if metric.time_granularity.name not in valid_granularities:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"`time_granularity` for metric '{metric.name}' must be >= "
                            f"{min_queryable_granularity.name}. Valid options are those that are >= the largest "
                            f"granularity defined for the metric's measures' agg_time_dimensions. Got: "
                            f"{metric.time_granularity.name}. Valid options: {valid_granularities}"
                        ),
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring metric time_granularitys are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Validate that the time_granularity for each metric is queryable for that metric.

        TODO: figure out a more efficient way to reference other aspects of the model. This validation essentially
        requires parsing the entire model, which could be slow and likely is repeated work. The blocker is that the
        inputs to validations are protocols, which don't easily store parsed metadata.
        """
        issues: List[ValidationIssue] = []

        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]] = {}
        for semantic_model in semantic_manifest.semantic_models:
            dimension_index = {DimensionReference(dimension.name): dimension for dimension in semantic_model.dimensions}
            for measure in semantic_model.measures:
                try:
                    agg_time_dimension_ref = semantic_model.checked_agg_time_dimension_for_measure(measure.reference)
                    agg_time_dimension: Optional[Dimension] = dimension_index[
                        agg_time_dimension_ref.dimension_reference
                    ]
                except (AssertionError, KeyError):
                    # If the agg_time_dimension is not set or does not exist, this will be validated elsewhere.
                    # Here, swallow the error to avoid disrupting the validation process.
                    agg_time_dimension = None
                measure_to_agg_time_dimension[measure.reference] = agg_time_dimension

        metric_index = {MetricReference(metric.name): metric for metric in semantic_manifest.metrics}
        for metric in semantic_manifest.metrics or []:
            issues += MetricTimeGranularityRule._validate_metric(
                metric=metric,
                metric_index=metric_index,
                measure_to_agg_time_dimension=measure_to_agg_time_dimension,
            )
        return issues
