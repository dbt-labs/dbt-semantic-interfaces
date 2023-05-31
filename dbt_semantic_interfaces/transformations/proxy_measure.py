import logging

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.metric import (
    MetricType,
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class CreateProxyMeasureRule(SemanticManifestTransformRule):
    """Adds a proxy metric for measures that have the create_metric flag set, if it does not already exist.

    Also checks that a defined metric with the same name as a measure is a proxy metric.
    """

    @staticmethod
    def transform_model(model: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Creates measure proxy metrics for measures with `create_metric==True`."""
        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                if not measure.create_metric:
                    continue

                add_metric = True
                for metric in model.metrics:
                    if metric.name == measure.name:
                        if metric.type != MetricType.MEASURE_PROXY:
                            raise ModelTransformError(
                                f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                                f"proxy for that measure"
                            )
                        logger.warning(
                            f"Metric already exists with name ({measure.name}). *Not* adding measure proxy metric for "
                            f"that measure"
                        )
                        add_metric = False

                if add_metric is True:
                    model.metrics.append(
                        PydanticMetric(
                            name=measure.name,
                            type=MetricType.MEASURE_PROXY,
                            type_params=PydanticMetricTypeParams(
                                measures=[PydanticMetricInputMeasure(name=measure.name)],
                                expr=measure.name,
                            ),
                        )
                    )

        return model
