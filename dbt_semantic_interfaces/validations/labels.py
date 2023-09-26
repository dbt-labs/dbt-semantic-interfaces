import logging
from typing import Dict, Generic, List, Sequence

from dbt_semantic_interfaces.protocols import Metric, SemanticManifestT
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class MetricLabelsRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the labels are unique across metrics."""

    @staticmethod
    @validate_safely("Checking that a metric has a unique label")
    def _check_metric(metric: Metric, existing_labels: Dict[str, str]) -> Sequence[ValidationIssue]:  # noqa: D
        if metric.label in existing_labels:
            return (
                ValidationError(
                    context=FileContext.from_metadata(metric.metadata),
                    message=f"Can't use label `{metric.label}` for  metric `{metric.name}` "
                    f"as it's already used for metric `{existing_labels[metric.label]}`",
                ),
            )
        elif metric.label is not None:
            existing_labels[metric.label] = metric.name

        return ()

    @staticmethod
    @validate_safely("Checking labels are unique across metrics")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        labels_to_metrics: Dict[str, str] = {}
        for metric in semantic_manifest.metrics:
            issues += MetricLabelsRule._check_metric(metric=metric, existing_labels=labels_to_metrics)

        return issues
