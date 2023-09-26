from typing import Protocol, runtime_checkable

from hypothesis import given
from hypothesis.strategies import builds, just, lists

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from dbt_semantic_interfaces.protocols import Dimension as DimensionProtocol
from dbt_semantic_interfaces.protocols import Entity as EntityProtocol
from dbt_semantic_interfaces.protocols import Measure as MeasureProtocol
from dbt_semantic_interfaces.protocols import Metadata as MetadataProtocol
from dbt_semantic_interfaces.protocols import Metric as MetricProtocol
from dbt_semantic_interfaces.protocols import SavedQuery as SavedQueryProtocol
from dbt_semantic_interfaces.protocols import (
    SemanticManifest as SemanticManifestProtocol,
)
from dbt_semantic_interfaces.protocols import SemanticModel as SemanticModelProtocol
from dbt_semantic_interfaces.protocols.time_spine_configuration import (
    TimeSpineTableConfiguration as TimeSpineTableConfigurationProtocol,
)
from dbt_semantic_interfaces.type_enums import MetricType

SIMPLE_METRIC_STRATEGY = builds(
    PydanticMetric,
    type=just(MetricType.SIMPLE),
    type_params=builds(PydanticMetricTypeParams, measure=builds(PydanticMetricInputMeasure)),
)

SEMANTIC_MODEL_STRATEGY = builds(
    PydanticSemanticModel,
    dimensions=lists(builds(PydanticDimension)),
    entities=lists(builds(PydanticEntity)),
    measures=lists(builds(PydanticMeasure)),
)


@runtime_checkable
class RuntimeCheckableSemanticManifest(SemanticManifestProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(
    builds(
        PydanticSemanticManifest,
        semantic_models=lists(SEMANTIC_MODEL_STRATEGY),
        metrics=lists(SIMPLE_METRIC_STRATEGY),
        saved_queries=lists(builds(PydanticSavedQuery)),
        project_configuration=builds(PydanticProjectConfiguration),
    )
)
def test_semantic_manifest_protocol(semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D
    assert isinstance(semantic_manifest, RuntimeCheckableSemanticManifest)


@runtime_checkable
class RuntimeCheckableSemanticModel(SemanticModelProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(SEMANTIC_MODEL_STRATEGY)
def test_semantic_model_protocol(semantic_model: PydanticSemanticModel) -> None:  # noqa: D
    assert isinstance(semantic_model, RuntimeCheckableSemanticModel)


@runtime_checkable
class RuntimeCheckableMetric(MetricProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(SIMPLE_METRIC_STRATEGY)
def test_metric_protocol_simple(metric: PydanticMetric) -> None:  # noqa: D
    assert isinstance(metric, RuntimeCheckableMetric)


@given(
    builds(
        PydanticMetric,
        type=just(MetricType.RATIO),
        type_params=builds(
            PydanticMetricTypeParams,
            numerator=builds(PydanticMetricInput),
            denominator=builds(PydanticMetricInput),
        ),
    )
)
def test_metric_protocol_ratio(metric: PydanticMetric) -> None:  # noqa: D
    assert isinstance(metric, RuntimeCheckableMetric)


@given(
    builds(
        PydanticMetric,
        type=just(MetricType.DERIVED),
        type_params=builds(PydanticMetricTypeParams, metrics=lists(builds(PydanticMetricInput))),
        expr=builds(str),
    )
)
def test_metric_protocol_derived(metric: PydanticMetric) -> None:  # noqa: D
    assert isinstance(metric, RuntimeCheckableMetric)


@runtime_checkable
class RuntimeCheckableEntity(EntityProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticEntity))
def test_entity_protocol(entity: PydanticEntity) -> None:  # noqa: D
    assert isinstance(entity, RuntimeCheckableEntity)


@runtime_checkable
class RuntimeCheckableMeasure(MeasureProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticMeasure))
def test_measure_protocol(measure: PydanticMeasure) -> None:  # noqa: D
    assert isinstance(measure, RuntimeCheckableMeasure)


@runtime_checkable
class RuntimeCheckableDimension(DimensionProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticDimension))
def test_dimension_protocol(dimesnion: PydanticDimension) -> None:  # noqa: D
    assert isinstance(dimesnion, RuntimeCheckableDimension)


@runtime_checkable
class RuntimeCheckableMetadata(MetadataProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticMetadata))
def test_metadata_protocol(metadata: PydanticMetadata) -> None:  # noqa: D
    assert isinstance(metadata, RuntimeCheckableMetadata)


@runtime_checkable
class RuntimeCheckableSavedQuery(SavedQueryProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticSavedQuery))
def test_saved_query_protocol(saved_query: PydanticSavedQuery) -> None:  # noqa: D
    assert isinstance(saved_query, RuntimeCheckableSavedQuery)


@runtime_checkable
class RuntimeCheckableTimeSpineConfiguration(TimeSpineTableConfigurationProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticTimeSpineTableConfiguration))
def test_time_spine_table_configuration_protocol(time_spine: PydanticTimeSpineTableConfiguration) -> None:  # noqa: D
    assert isinstance(time_spine, RuntimeCheckableTimeSpineConfiguration)
