from copy import deepcopy

import pytest

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.test_utils import find_metric_with
from dbt_semantic_interfaces.validations.labels import MetricLabelsRule
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)


def test_metric_label_happy_path(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    SemanticManifestValidator[PydanticSemanticManifest](
        [MetricLabelsRule[PydanticSemanticManifest]()]
    ).checked_validations(manifest)


def test_duplicate_metric_label(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    metric = find_metric_with(manifest, lambda metric: metric.label is not None)
    duplicated_metric, _ = deepcopy(metric)
    duplicated_metric.name = duplicated_metric.name + "_copy"
    manifest.metrics.append(duplicated_metric)
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Can't use label `{duplicated_metric.label}` for  metric",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [MetricLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)
