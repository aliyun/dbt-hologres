from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
)
from dbt_common.dataclass_schema import dbtClassMixin, ValidationError
from dbt_common.utils import encoding as dbt_encoding
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError


@dataclass(frozen=True, eq=True)
class HologresIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def __hash__(self) -> int:
        """Custom hash to handle List[str] columns field."""
        return hash((tuple(self.columns), self.unique, self.type))

    def __eq__(self, other: object) -> bool:
        """Custom equality to handle List[str] columns field."""
        if not isinstance(other, HologresIndexConfig):
            return NotImplemented
        return (
            self.columns == other.columns
            and self.unique == other.unique
            and self.type == other.type
        )

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run.
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        inputs = self.columns + [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["HologresIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass(frozen=True)
class HologresIndexConfigChange(RelationConfigChange):
    context: HologresIndexConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False
