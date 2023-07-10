from typing import Protocol, runtime_checkable

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
    PydanticDimensionValidityParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metadata import (
    PydanticFileSlice,
    PydanticMetadata,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import (
    NodeRelation,
    PydanticSemanticModel,
)
from dbt_semantic_interfaces.protocols import Dimension as DimensionProtocol
from dbt_semantic_interfaces.protocols import Entity as EntityProtocol
from dbt_semantic_interfaces.protocols import Measure as MeasureProtocol
from dbt_semantic_interfaces.protocols import Metadata as MetadataProtocol
from dbt_semantic_interfaces.protocols import Metric as MetricProtocol
from dbt_semantic_interfaces.protocols import (
    SemanticManifest as SemanticManifestProtocol,
)
from dbt_semantic_interfaces.protocols import SemanticModel as SemanticModelProtocol
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


@runtime_checkable
class RuntimeCheckableSemanticManifest(SemanticManifestProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_semantic_manifest_protocol() -> None:  # noqa: D
    semantic_model = PydanticSemanticModel(
        name="test_semantic_model",
        node_relation=NodeRelation(
            alias="test_alias",
            schema="test_schema_name",
        ),
        entities=[],
        measures=[],
        dimensions=[],
    )
    metric = PydanticMetric(
        name="test_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="test_measure")),
    )
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )
    assert isinstance(semantic_manifest, RuntimeCheckableSemanticManifest)


@runtime_checkable
class RuntimeCheckableSemanticModel(SemanticModelProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_semantic_model_protocol() -> None:  # noqa: D
    test_semantic_model = PydanticSemanticModel(
        name="test_semantic_model",
        node_relation=NodeRelation(
            alias="test_alias",
            schema="test_schema_name",
        ),
        entities=[],
        measures=[],
        dimensions=[],
    )
    assert isinstance(test_semantic_model, RuntimeCheckableSemanticModel)


@runtime_checkable
class RuntimeCheckableMetric(MetricProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_metric_protocol() -> None:  # noqa: D
    test_metric = PydanticMetric(
        name="test_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="test_measure")),
    )
    assert isinstance(test_metric, RuntimeCheckableMetric)


@runtime_checkable
class RuntimeCheckableEntity(EntityProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_entity_protocol() -> None:  # noqa: D
    test_entity = PydanticEntity(
        name="test_name",
        type=EntityType.PRIMARY,
    )
    assert isinstance(test_entity, RuntimeCheckableEntity)


@runtime_checkable
class RuntimeCheckableMeasure(MeasureProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_measure_protocol() -> None:  # noqa: D
    test_measure = PydanticMeasure(
        name="test_measure",
        agg=AggregationType.SUM,
        agg_time_dimension="some_time_dimension",
    )
    assert isinstance(test_measure, RuntimeCheckableMeasure)


@runtime_checkable
class RuntimeCheckableDimension(DimensionProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_dimension_protocol() -> None:  # noqa: D
    time_dim = PydanticDimension(
        name="test_time_dim",
        type=DimensionType.TIME,
        type_params=PydanticDimensionTypeParams(
            time_granularity=TimeGranularity.DAY,
            validity_params=PydanticDimensionValidityParams(),
        ),
    )
    assert isinstance(time_dim, RuntimeCheckableDimension)

    # Skipping this assertion because are implementation of the function `time_dimension_reference` raises an
    # exception if DimensionType != TIME. The isinstance check seems to actually run the function thus
    # raising an exception during the assertion.
    # of
    # categorical_dim = PydanticDimension(
    #     name="test_categorical_dim",
    #     type=DimensionType.CATEGORICAL,
    # )
    # assert isinstance(categorical_dim, RuntimeCheckableDimension)


@runtime_checkable
class RuntimeCheckableMetadata(MetadataProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_metadata_protocol() -> None:  # noqa: D
    metadata = PydanticMetadata(
        repo_file_path="/path/to/cats.txt",
        file_slice=PydanticFileSlice(
            filename="cats.txt",
            content="I like cats",
            start_line_number=0,
            end_line_number=1,
        ),
    )
    assert isinstance(metadata, RuntimeCheckableMetadata)
