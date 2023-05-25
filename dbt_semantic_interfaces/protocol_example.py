import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, Sequence

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel

logger = logging.getLogger(__name__)


# Protocols
class SemanticModel(Protocol):  # noqa: D
    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass


class SemanticManifest(Protocol):  # noqa: D
    @property
    @abstractmethod
    def semantic_models(self) -> Sequence[SemanticModel]:  # noqa: D
        raise NotImplementedError


# Explicit Pydantic BaseModel implementations - throws error:
# TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of
# all its bases

# class ExplicitPydanticSemanticModel(FrozenBaseModel, SemanticModel):  # noqa: D
#     name: str


# class PydanticSemanticManifest(SemanticManifest, FrozenBaseModel):  # noqa: D
#     semantic_models: Sequence[PydanticSemanticModel]
#
#


# Implicit Pydantic implementation
class ImplicitPydanticSemanticModel(FrozenBaseModel):  # noqa: D
    name: str


class ImplicitPydanticSemanticManifest(FrozenBaseModel):  # noqa: D
    semantic_models: Sequence[ImplicitPydanticSemanticModel]


# Explicit Dataclass implementation
# Throws error:
# TypeError: Can't instantiate abstract class ExplicitDataclassSemanticModel with abstract methods name

# @dataclass(frozen=True)
# class ExplicitDataclassSemanticModel(SemanticModel):  # noqa: D
#     name: str
#
#
# @dataclass(frozen=True)
# class ExplicitDataclassSemanticManifest(SemanticManifest):  # noqa: D
#     semantic_models: Sequence[ExplicitDataclassSemanticModel]


# Implicit Dataclass implementation
@dataclass(frozen=True)
class ImplicitDataclassSemanticModel:  # noqa: D
    name: str


@dataclass(frozen=True)
class ImplicitDataclassSemanticManifest:  # noqa: D
    semantic_models: Sequence[ImplicitDataclassSemanticModel]


# Explicit plain Python implementation
class ExplicitPlainSemanticModel(SemanticModel):  # noqa: D
    @property
    def name(self) -> str:  # noqa: D
        return "foo"


class ExplicitPlainSemanticManifest(SemanticManifest):  # noqa: D
    @property
    def semantic_models(self) -> Sequence[ExplicitPlainSemanticModel]:  # noqa: D
        return [ExplicitPlainSemanticModel()]


# Implicit plain Python implementation
class ImplicitPlainSemanticModel:  # noqa: D
    @property
    def name(self) -> str:  # noqa: D
        return "foo"


class ImplicitPlainSemanticManifest:  # noqa: D
    @property
    def semantic_models(self) -> Sequence[ImplicitPlainSemanticModel]:  # noqa: D
        return [ImplicitPlainSemanticModel()]


# Invalid implementation - should throw type error
class InvalidImplicitPydanticSemanticManifest(FrozenBaseModel):  # noqa: D
    class_type_check_error: str


def check_function(semantic_manifest: SemanticManifest) -> None:  # noqa: D
    for semantic_model in semantic_manifest.semantic_models:
        logger.error(semantic_model.name)
