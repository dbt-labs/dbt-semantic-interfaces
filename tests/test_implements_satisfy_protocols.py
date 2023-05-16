from typing import Protocol, runtime_checkable

from dbt_semantic_interfaces.objects.elements.dimension import (
    Dimension,
    DimensionTypeParams,
    DimensionValidityParams,
)
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metadata import FileSlice, Metadata
from dbt_semantic_interfaces.protocols.dimension import Dimension as DimensionProtocol
from dbt_semantic_interfaces.protocols.entity import Entity as EntityProtocol
from dbt_semantic_interfaces.protocols.measure import Measure as MeasureProtocol
from dbt_semantic_interfaces.protocols.metadata import Metadata as MetadataProtocol
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.entity_type import EntityType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


@runtime_checkable
class RuntimeCheckableEntity(EntityProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_entity_protocol() -> None:  # noqa: D
    test_entity = Entity(
        name="test_name",
        type=EntityType.PRIMARY,
    )
    assert isinstance(test_entity, RuntimeCheckableEntity)


@runtime_checkable
class RuntimeCheckableMeasure(MeasureProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_measure_protocol() -> None:  # noqa: D
    test_measure = Measure(
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
    time_dim = Dimension(
        name="test_time_dim",
        type=DimensionType.TIME,
        type_params=DimensionTypeParams(
            time_granularity=TimeGranularity.DAY,
            validity_params=DimensionValidityParams(),
        ),
    )
    assert isinstance(time_dim, RuntimeCheckableDimension)

    # Skipping this assertion because are implementation of the function `time_dimension_reference` raises an
    # exception if DimensionType != TIME. The isinstance check seems to actually run the function thus
    # raising an exception during the assertion.
    # of
    # categorical_dim = Dimension(
    #     name="test_categorical_dim",
    #     type=DimensionType.CATEGORICAL,
    # )
    # assert isinstance(categorical_dim, RuntimeCheckableDimension)


@runtime_checkable
class RuntimeCheckableMetadata(MetadataProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


def test_metadata_protocol() -> None:  # noqa: D
    metadata = Metadata(
        repo_file_path="/path/to/cats.txt",
        file_slice=FileSlice(
            filename="cats.txt",
            content="I like cats",
            start_line_number=0,
            end_line_number=1,
        ),
    )
    assert isinstance(metadata, RuntimeCheckableMetadata)
