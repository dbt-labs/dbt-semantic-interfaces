import logging
from typing import Sequence

from dbt_semantic_interfaces.transformations.add_input_metric_measures import (
    AddInputMetricMeasuresRule,
)
from dbt_semantic_interfaces.transformations.agg_time_dimension import (
    SetMeasureAggregationTimeDimensionRule,
)
from dbt_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
from dbt_semantic_interfaces.transformations.convert_median import (
    ConvertMedianToPercentileRule,
)
from dbt_semantic_interfaces.transformations.names import LowerCaseNamesRule
from dbt_semantic_interfaces.transformations.proxy_measure import CreateProxyMeasureRule
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class PydanticSemanticManifestTransformRuleSet:
    """Transform rules that should be used for the Pydantic implementation of SemanticManifest."""

    @property
    def primary_rules(self) -> Sequence[SemanticManifestTransformRule]:  # noqa:
        return (
            LowerCaseNamesRule(),
            SetMeasureAggregationTimeDimensionRule(),
        )

    @property
    def secondary_rules(self) -> Sequence[SemanticManifestTransformRule]:  # noqa: D
        return (
            CreateProxyMeasureRule(),
            BooleanMeasureAggregationRule(),
            ConvertCountToSumRule(),
            ConvertMedianToPercentileRule(),
            AddInputMetricMeasuresRule(),
        )

    @property
    def all_rules(self) -> Sequence[Sequence[SemanticManifestTransformRule]]:  # noqa: D
        return self.primary_rules, self.secondary_rules
