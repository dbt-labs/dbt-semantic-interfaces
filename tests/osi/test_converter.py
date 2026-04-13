from __future__ import annotations

import json

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import (
    PydanticNodeRelation,
    PydanticSemanticModel,
)
from dbt_semantic_interfaces.osi.converter import DSIToOSIConverter
from dbt_semantic_interfaces.osi.models import OSIDialect
from dbt_semantic_interfaces.test_utils import (
    default_meta,
    semantic_model_with_guaranteed_meta,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    TimeGranularity,
)


def _manifest(
    semantic_models: list[PydanticSemanticModel] | None = None,
) -> PydanticSemanticManifest:
    return PydanticSemanticManifest(
        semantic_models=semantic_models or [],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(),
    )


def _dimension(
    name: str,
    dim_type: DimensionType = DimensionType.CATEGORICAL,
    expr: str | None = None,
    description: str | None = None,
    label: str | None = None,
    granularity: TimeGranularity | None = None,
) -> PydanticDimension:
    type_params = PydanticDimensionTypeParams(time_granularity=granularity) if granularity else None
    return PydanticDimension(
        name=name,
        type=dim_type,
        expr=expr,
        description=description,
        label=label,
        type_params=type_params,
        metadata=default_meta(),
        config=None,
    )


def _measure(
    name: str,
    agg: AggregationType = AggregationType.SUM,
    expr: str | None = None,
    description: str | None = None,
    label: str | None = None,
) -> PydanticMeasure:
    return PydanticMeasure(
        name=name,
        agg=agg,
        expr=expr,
        description=description,
        label=label,
        create_metric=None,
        agg_params=None,
        metadata=default_meta(),
    )


def _entity(
    name: str,
    entity_type: EntityType = EntityType.PRIMARY,
    expr: str | None = None,
) -> PydanticEntity:
    return PydanticEntity(
        name=name,
        type=entity_type,
        expr=expr,
        description=None,
        role=None,
        config=None,
    )


class TestBasicConversion:  # noqa: D
    def test_empty_manifest_produces_empty_datasets(self) -> None:  # noqa: D
        result = DSIToOSIConverter().convert(_manifest(), model_name="test")

        assert result.version == "1.0"
        assert len(result.semantic_model) == 1
        assert result.semantic_model[0].name == "test"
        assert result.semantic_model[0].datasets == []
        assert result.semantic_model[0].metrics is None
        assert result.semantic_model[0].relationships is None

    def test_semantic_model_becomes_dataset(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            description="Order data",
            node_relation=PydanticNodeRelation(schema_name="analytics", alias="orders_table"),
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        dataset = result.semantic_model[0].datasets[0]
        assert dataset.name == "orders"
        assert dataset.source == "analytics.orders_table"
        assert dataset.description == "Order data"

    def test_source_includes_database_when_present(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            node_relation=PydanticNodeRelation(schema_name="analytics", alias="orders_table", database="prod"),
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].source == "prod.analytics.orders_table"

    def test_multiple_semantic_models_become_multiple_datasets(self) -> None:  # noqa: D
        sm_a = semantic_model_with_guaranteed_meta(name="orders")
        sm_b = semantic_model_with_guaranteed_meta(name="users")
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm_a, sm_b]))

        names = [ds.name for ds in result.semantic_model[0].datasets]
        assert names == ["orders", "users"]


class TestDimensionConversion:  # noqa: D
    def test_categorical_dimension_has_is_time_false(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status", dim_type=DimensionType.CATEGORICAL)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "status"
        assert field.dimension is not None
        assert field.dimension.is_time is False

    def test_time_dimension_has_is_time_true(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("ds", dim_type=DimensionType.TIME, granularity=TimeGranularity.DAY)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.dimension is not None
        assert field.dimension.is_time is True

    def test_dimension_with_expr_uses_expr(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("order_date", expr="DATE(created_at)")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "DATE(created_at)"

    def test_dimension_without_expr_falls_back_to_name(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "status"

    def test_dimension_description_and_label_carried_over(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status", description="Order status", label="Status")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].description == "Order status"
        assert fields[0].label == "Status"


class TestMeasureConversion:  # noqa: D
    def test_measure_becomes_field_without_dimension_metadata(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "revenue"
        assert field.expression.dialects[0].expression == "amount"
        assert field.dimension is None

    def test_measure_without_expr_falls_back_to_name(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("num_orders", agg=AggregationType.COUNT)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "num_orders"

    def test_measure_description_and_label_carried_over(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", description="Total revenue", label="Revenue")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].description == "Total revenue"
        assert fields[0].label == "Revenue"


class TestEntityConversion:  # noqa: D
    def test_entity_becomes_field(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "order_id"
        assert field.expression.dialects[0].expression == "order_id"
        assert field.dimension is None

    def test_entity_with_expr_uses_expr(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY, expr="id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "order_id"
        assert field.expression.dialects[0].expression == "id"

    def test_foreign_entity_also_becomes_field(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user_id", entity_type=EntityType.FOREIGN)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].name == "user_id"


class TestEntityKeyExtraction:  # noqa: D
    def test_primary_entity_becomes_primary_key(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].primary_key == ["order_id"]

    def test_primary_entity_with_expr_uses_expr_in_key(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY, expr="id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].primary_key == ["id"]

    def test_unique_entity_becomes_unique_key(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("email", entity_type=EntityType.UNIQUE)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].unique_keys == [["email"]]

    def test_foreign_entity_does_not_appear_in_keys(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user_id", entity_type=EntityType.FOREIGN)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        dataset = result.semantic_model[0].datasets[0]
        assert dataset.primary_key is None
        assert dataset.unique_keys is None


class TestFieldOrdering:  # noqa: D
    def test_entities_then_dimensions_then_measures(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
            dimensions=[_dimension("status")],
            measures=[_measure("revenue", expr="amount")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].name == "order_id"
        assert fields[1].name == "status"
        assert fields[2].name == "revenue"


class TestDialectConfiguration:  # noqa: D
    def test_default_dialect_is_ansi_sql(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.dialects == [OSIDialect.ANSI_SQL]
        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].dialect == OSIDialect.ANSI_SQL

    def test_configurable_dialect(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = DSIToOSIConverter(dialect=OSIDialect.SNOWFLAKE).convert(_manifest(semantic_models=[sm]))

        assert result.dialects == [OSIDialect.SNOWFLAKE]
        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].dialect == OSIDialect.SNOWFLAKE


class TestRelationshipConversion:  # noqa: D
    def test_shared_entity_name_produces_relationship(self) -> None:  # noqa: D
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="listing_id")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 1
        rel = rels[0]
        assert rel.from_dataset == "listings"
        assert rel.to == "bookings"
        assert rel.from_columns == ["listing_id"]
        assert rel.to_columns == ["listing_id"]

    def test_same_type_entities_produce_relationship(self) -> None:  # noqa: D
        users_a = semantic_model_with_guaranteed_meta(
            name="users_a",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="user_id")],
        )
        users_b = semantic_model_with_guaranteed_meta(
            name="users_b",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="uid")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[users_a, users_b]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 1
        assert rels[0].from_columns == ["user_id"]
        assert rels[0].to_columns == ["uid"]

    def test_single_dataset_with_entity_produces_no_relationship(self) -> None:  # noqa: D
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[bookings]))

        assert result.semantic_model[0].relationships is None

    def test_same_dataset_entities_excluded(self) -> None:  # noqa: D
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[
                _entity("order", entity_type=EntityType.PRIMARY, expr="order_id"),
                _entity("order", entity_type=EntityType.FOREIGN, expr="order_id"),
            ],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[orders]))

        assert result.semantic_model[0].relationships is None

    def test_three_datasets_produce_all_pairs(self) -> None:  # noqa: D
        users_a = semantic_model_with_guaranteed_meta(
            name="users_a",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="user_id")],
        )
        users_b = semantic_model_with_guaranteed_meta(
            name="users_b",
            entities=[_entity("user", entity_type=EntityType.UNIQUE, expr="user_id")],
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user", entity_type=EntityType.FOREIGN, expr="user_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[users_a, users_b, orders]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 3
        pairs = {(r.from_dataset, r.to) for r in rels}
        assert pairs == {("users_a", "users_b"), ("users_a", "orders"), ("users_b", "orders")}

    def test_columns_use_expr_when_present(self) -> None:  # noqa: D
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="lid")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="fk_lid")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        rel = rels[0]
        assert rel.from_columns == ["lid"]
        assert rel.to_columns == ["fk_lid"]

    def test_columns_fall_back_to_name_without_expr(self) -> None:  # noqa: D
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY)],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].from_columns == ["listing"]
        assert rels[0].to_columns == ["listing"]

    def test_primary_entity_shorthand_does_not_produce_relationship(self) -> None:  # noqa: D
        bookings = PydanticSemanticModel(
            name="bookings",
            description=None,
            node_relation=PydanticNodeRelation(schema_name="schema", alias="table"),
            primary_entity="booking",
            entities=[],
            metadata=default_meta(),
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("booking", entity_type=EntityType.FOREIGN, expr="booking_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[bookings, orders]))

        assert result.semantic_model[0].relationships is None

    def test_relationship_name_format(self) -> None:  # noqa: D
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="listing_id")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].name == "listings__bookings__listing"

    def test_natural_entity_excluded(self) -> None:  # noqa: D
        users = semantic_model_with_guaranteed_meta(
            name="users",
            entities=[_entity("user", entity_type=EntityType.NATURAL, expr="user_id")],
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user", entity_type=EntityType.FOREIGN, expr="user_id")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[users, orders]))

        assert result.semantic_model[0].relationships is None

    def test_direction_follows_manifest_order(self) -> None:  # noqa: D
        alpha = semantic_model_with_guaranteed_meta(
            name="alpha",
            entities=[_entity("shared", entity_type=EntityType.FOREIGN, expr="col_a")],
        )
        beta = semantic_model_with_guaranteed_meta(
            name="beta",
            entities=[_entity("shared", entity_type=EntityType.PRIMARY, expr="col_b")],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[alpha, beta]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].from_dataset == "alpha"
        assert rels[0].to == "beta"


class TestOSIJsonSerialization:  # noqa: D
    def test_to_osi_json_produces_valid_json(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            description="Order data",
            dimensions=[_dimension("ds", dim_type=DimensionType.TIME, granularity=TimeGranularity.DAY)],
            measures=[_measure("revenue", expr="amount")],
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]), model_name="my_project")
        parsed = json.loads(result.to_osi_json())

        assert parsed["version"] == "1.0"
        assert len(parsed["semantic_model"]) == 1
        assert parsed["semantic_model"][0]["name"] == "my_project"

    def test_to_osi_json_excludes_none_fields(self) -> None:  # noqa: D
        sm = semantic_model_with_guaranteed_meta(name="orders")
        result = DSIToOSIConverter().convert(_manifest(semantic_models=[sm]))
        parsed = json.loads(result.to_osi_json())

        dataset = parsed["semantic_model"][0]["datasets"][0]
        assert "primary_key" not in dataset
        assert "unique_keys" not in dataset
        assert "fields" not in dataset
        assert "description" not in dataset
