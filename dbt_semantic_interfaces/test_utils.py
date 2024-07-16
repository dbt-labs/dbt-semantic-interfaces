import datetime
import logging
import textwrap
from typing import Callable, Optional, Sequence, Tuple

import dateutil.parser

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metadata import (
    PydanticFileSlice,
    PydanticMetadata,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import (
    PydanticNodeRelation,
    PydanticSemanticModel,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity

logger = logging.getLogger(__name__)


def as_datetime(date_string: str) -> datetime.datetime:
    """Helper to convert a string like '2020-01-01' into a datetime object."""
    return dateutil.parser.parse(date_string)


def find_semantic_model_with(
    model: PydanticSemanticManifest, function: Callable[[PydanticSemanticModel], bool]
) -> Tuple[PydanticSemanticModel, int]:
    """Returns a semantic model from the model which matches the criteria defined by the passed in function'.

    This is useful because the order of semantic models in the list is non determinant, thus it's impossible to
    hard code which semantic model you want by index. Using semantic model names isn't great for consistency because
    semantic models change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a semantic model will be returned which meets the requirements of what a test needs,
    unless none of the semantic models will work.
    """
    for index, semantic_model in enumerate(model.semantic_models):
        if function(semantic_model):
            return semantic_model, index

    raise Exception("Unable to find a semantic_model matching function criteria")


def find_metric_with(
    model: PydanticSemanticManifest, function: Callable[[PydanticMetric], bool]
) -> Tuple[PydanticMetric, int]:
    """Returns a metric from the model which matches the criteria defined by the passed in function'.

    This is useful because the order of metrics in the list is non-determinant, thus it's impossible to
    hard code which metric you want by index. Using metric names isn't great for consistency because
    metrics change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a metric will be returned which meets the requirements of what a test needs,
    unless none of the metrics will work.
    """
    for index, metric in enumerate(model.metrics):
        if function(metric):
            return metric, index

    raise Exception("Unable to find a metric matching function criteria")


def base_semantic_manifest_file() -> YamlConfigFile:
    """Returns a YamlConfigFile with the inputs for a basic valid semantic manifest.

    This is useful to seed a simple error-free semantic manifest, which can easily be extended with YAML inputs
    containing specific validation triggers.
    """
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              agg_time_dimension: ds
              expr: 1
              create_metric: true
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    return YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)


def default_meta() -> PydanticMetadata:
    """Returns a Metadata object with the required information."""
    return PydanticMetadata(
        repo_file_path="/not/from/a/repo",
        file_slice=PydanticFileSlice(
            filename="not_from_file.py",
            content="N/A",
            start_line_number=0,
            end_line_number=0,
        ),
    )


def metric_with_guaranteed_meta(
    name: str,
    type: MetricType,
    type_params: PydanticMetricTypeParams,
    metadata: PydanticMetadata = default_meta(),
    description: str = "adhoc metric",
    time_granularity: Optional[TimeGranularity] = None,
) -> PydanticMetric:
    """Creates a metric with the given input.

    If a metadata object is not supplied, a default metadata object is used.
    """
    return PydanticMetric(
        name=name,
        description=description,
        type=type,
        type_params=type_params,
        filter=None,
        metadata=metadata,
        time_granularity=time_granularity,
    )


def semantic_model_with_guaranteed_meta(
    name: str,
    description: Optional[str] = None,
    node_relation: Optional[PydanticNodeRelation] = None,
    metadata: PydanticMetadata = default_meta(),
    entities: Sequence[PydanticEntity] = (),
    measures: Sequence[PydanticMeasure] = (),
    dimensions: Sequence[PydanticDimension] = (),
) -> PydanticSemanticModel:
    """Creates a semantic model with the given input.

    If a metadata object is not supplied, a default metadata object is used.
    """
    created_node_relation = node_relation
    if created_node_relation is None:
        created_node_relation = PydanticNodeRelation(
            schema_name="schema",
            alias="table",
        )

    return PydanticSemanticModel(
        name=name,
        description=description,
        node_relation=created_node_relation,
        entities=entities,
        measures=measures,
        dimensions=dimensions,
        metadata=metadata,
    )
