from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.osi.models import (
    OSIDataset,
    OSIDialect,
    OSIDialectExpression,
    OSIDimension,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIRelationship,
    OSISemanticModel,
)
from dbt_semantic_interfaces.protocols.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType


class DSIToOSIConverter:
    """Converts a DSI SemanticManifest into an OSI Document."""

    def __init__(self, dialect: OSIDialect = OSIDialect.ANSI_SQL) -> None:  # noqa: D
        self._dialect = dialect

    def convert(self, manifest: SemanticManifest, model_name: str = "semantic_model") -> OSIDocument:  # noqa: D
        datasets = [self._convert_semantic_model(sm) for sm in manifest.semantic_models]

        entity_index = self._build_entity_index(manifest.semantic_models)
        relationships = self._build_relationships(entity_index)

        return OSIDocument(
            version="1.0",
            dialects=[self._dialect],
            semantic_model=[
                OSISemanticModel(
                    name=model_name,
                    datasets=datasets,
                    relationships=relationships if relationships else None,
                )
            ],
        )

    def _convert_semantic_model(self, sm: SemanticModel) -> OSIDataset:
        fields: List[OSIField] = []
        for entity in sm.entities:
            fields.append(self._convert_entity(entity))
        for dim in sm.dimensions:
            fields.append(self._convert_dimension(dim))
        for measure in sm.measures:
            fields.append(self._convert_measure(measure))

        primary_key, unique_keys = self._extract_keys(sm.entities)

        return OSIDataset(
            name=sm.name,
            source=sm.node_relation.relation_name,
            primary_key=primary_key,
            unique_keys=unique_keys if unique_keys else None,
            description=sm.description,
            fields=fields if fields else None,
        )

    def _convert_dimension(self, dim: Dimension) -> OSIField:
        expr = dim.expr if dim.expr is not None else dim.name
        is_time = dim.type == DimensionType.TIME

        return OSIField(
            name=dim.name,
            expression=self._make_expression(expr),
            dimension=OSIDimension(is_time=is_time),
            label=dim.label,
            description=dim.description,
        )

    def _convert_entity(self, entity: Entity) -> OSIField:
        expr = entity.expr if entity.expr is not None else entity.name

        return OSIField(
            name=entity.name,
            expression=self._make_expression(expr),
            label=entity.label,
            description=entity.description,
        )

    def _convert_measure(self, measure: Measure) -> OSIField:
        expr = measure.expr if measure.expr is not None else measure.name

        return OSIField(
            name=measure.name,
            expression=self._make_expression(expr),
            label=measure.label,
            description=measure.description,
        )

    @staticmethod
    def _extract_keys(entities: Sequence[Entity]) -> Tuple[Optional[List[str]], List[List[str]]]:
        primary_key: Optional[List[str]] = None
        unique_keys: List[List[str]] = []

        for entity in entities:
            col = entity.expr if entity.expr is not None else entity.name
            if entity.type == EntityType.PRIMARY:
                primary_key = [col]
            elif entity.type == EntityType.UNIQUE:
                unique_keys.append([col])

        return primary_key, unique_keys

    @staticmethod
    def _build_entity_index(
        semantic_models: Sequence[SemanticModel],
    ) -> Dict[str, List[Tuple[str, str]]]:
        """Map each entity name to the (dataset_name, column) pairs that declare it."""
        index: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for sm in semantic_models:
            for entity in sm.entities:
                if entity.type == EntityType.NATURAL:
                    continue
                col = entity.expr if entity.expr is not None else entity.name
                index[entity.name].append((sm.name, col))
        return dict(index)

    @staticmethod
    def _build_relationships(
        entity_index: Dict[str, List[Tuple[str, str]]],
    ) -> List[OSIRelationship]:
        """Resolve implicit DSI entity links into explicit OSI relationships.

        Every pair of datasets sharing an entity name is a valid join path.
        """
        relationships: List[OSIRelationship] = []
        for entity_name, entries in entity_index.items():
            for (ds_a, col_a), (ds_b, col_b) in combinations(entries, 2):
                if ds_a == ds_b:
                    continue
                relationships.append(
                    OSIRelationship(
                        name=f"{ds_a}__{ds_b}__{entity_name}",
                        from_dataset=ds_a,
                        to=ds_b,
                        from_columns=[col_a],
                        to_columns=[col_b],
                    )
                )
        return relationships

    def _make_expression(self, expr: str) -> OSIExpression:
        return OSIExpression(dialects=[OSIDialectExpression(dialect=self._dialect, expression=expr)])
