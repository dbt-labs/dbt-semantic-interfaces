from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from dbt_semantic_interfaces.protocols.export import Export
from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.protocols.where_filter import WhereFilterIntersection


class SavedQueryQueryParams(Protocol):
    """The parameters that will be passed into the MF query."""

    @property
    @abstractmethod
    def metrics(self) -> Sequence[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def group_by(self) -> Sequence[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def where(self) -> Optional[WhereFilterIntersection]:
        """Returns the intersection class containing any where filters specified in the saved query."""
        pass


class SavedQuery(Protocol):
    """Represents a query that the user wants to run repeatedly."""

    @property
    @abstractmethod
    def metadata(self) -> Optional[Metadata]:  # noqa: D
        pass

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def query_params(self) -> SavedQueryQueryParams:
        """Parameters that should be passed into the MF query."""
        pass

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """Returns a string representing a human readable label for the saved query."""
        pass

    @property
    @abstractmethod
    def exports(self) -> Sequence[Export]:
        """Exports that can run using this saved query."""
        pass
