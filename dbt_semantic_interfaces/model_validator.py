import copy
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Sequence

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.agg_time_dimension import (
    AggregationTimeDimensionRule,
)
from dbt_semantic_interfaces.validations.dimension_const import DimensionConsistencyRule
from dbt_semantic_interfaces.validations.element_const import ElementConsistencyRule
from dbt_semantic_interfaces.validations.entities import (
    NaturalEntityConfigurationRule,
    OnePrimaryEntityPerSemanticModelRule,
)
from dbt_semantic_interfaces.validations.measures import (
    CountAggregationExprRule,
    MeasureConstraintAliasesRule,
    MeasuresNonAdditiveDimensionRule,
    MetricMeasuresRule,
    PercentileAggregationRule,
    SemanticModelMeasuresUniqueRule,
)
from dbt_semantic_interfaces.validations.metrics import (
    CumulativeMetricRule,
    DerivedMetricRule,
)
from dbt_semantic_interfaces.validations.non_empty import NonEmptyRule
from dbt_semantic_interfaces.validations.reserved_keywords import ReservedKeywordsRule
from dbt_semantic_interfaces.validations.semantic_models import (
    SemanticModelTimeDimensionWarningsRule,
    SemanticModelValidityWindowRule,
)
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationException,
    SemanticManifestValidationResults,
    SemanticManifestValidationRule,
)

logger = logging.getLogger(__name__)


def _validate_manifest_with_one_rule(
    validation_rule: SemanticManifestValidationRule, semantic_manifest: SemanticManifest
) -> str:
    """Helper function to run a single validation rule on a semantic mode.

    Result is returned as a serialized object as there are pickling issues with SemanticManifestValidationResults.
    """
    return SemanticManifestValidationResults.from_issues_sequence(
        validation_rule.validate_model(semantic_manifest)
    ).json()


class SemanticManifestValidator:
    """A Validator that acts on SemanticManifest."""

    DEFAULT_RULES = (
        PercentileAggregationRule(),
        DerivedMetricRule(),
        CountAggregationExprRule(),
        SemanticModelMeasuresUniqueRule(),
        SemanticModelTimeDimensionWarningsRule(),
        SemanticModelValidityWindowRule(),
        DimensionConsistencyRule(),
        ElementConsistencyRule(),
        NaturalEntityConfigurationRule(),
        OnePrimaryEntityPerSemanticModelRule(),
        MeasureConstraintAliasesRule(),
        MetricMeasuresRule(),
        CumulativeMetricRule(),
        NonEmptyRule(),
        UniqueAndValidNameRule(),
        AggregationTimeDimensionRule(),
        ReservedKeywordsRule(),
        MeasuresNonAdditiveDimensionRule(),
    )

    def __init__(self, rules: Sequence[SemanticManifestValidationRule] = DEFAULT_RULES, max_workers: int = 1) -> None:
        """Constructor.

        Args:
            rules: List of validation rules to run. Defaults to DEFAULT_RULES
            max_workers: sets the max number of rules to run against the model concurrently
        """
        # Raises an error if 'rules' is an empty sequence or None
        if not rules:
            raise ValueError(
                "SemanticManifestValidator 'rules' must be a sequence with at least one SemanticManifestValidationRule."
            )

        self._rules = rules
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def validate_model(self, semantic_manifest: SemanticManifest) -> SemanticManifestValidationResults:
        """Validate a model according to configured rules."""
        results: List[SemanticManifestValidationResults] = []

        futures = [
            self._executor.submit(_validate_manifest_with_one_rule, validation_rule, semantic_manifest)
            for validation_rule in self._rules
        ]
        for future in as_completed(futures):
            res = future.result()
            result = SemanticManifestValidationResults.parse_raw(res)
            results.append(result)

        return SemanticManifestValidationResults.merge(results)

    def checked_validations(self, model: SemanticManifest) -> None:
        """Similar to validate(), but throws an exception if validation fails."""
        model_copy = copy.deepcopy(model)
        model_issues = self.validate_model(model_copy)
        if model_issues.has_blocking_issues:
            raise ModelValidationException(issues=tuple(model_issues.all_issues))
