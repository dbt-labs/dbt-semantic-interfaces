from abc import abstractmethod
from typing import Protocol


class WhereFilter(Protocol):
    """A filter that is applied using a WHERE filter in the generated SQL."""

    @property
    @abstractmethod
    def where_sql_template(self) -> str:
        """A template that describes how to render the SQL for a WHERE clause."""
        pass
