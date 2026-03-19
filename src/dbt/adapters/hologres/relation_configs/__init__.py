# Maximum characters in a Hologres identifier (same as PostgreSQL)
MAX_CHARACTERS_IN_IDENTIFIER = 90 

from dbt.adapters.hologres.relation_configs.index import (  # noqa: F401
    HologresIndexConfig,
    HologresIndexConfigChange,
)
from dbt.adapters.hologres.relation_configs.dynamic_table import (  # noqa: F401
    HologresDynamicTableConfig,
    HologresDynamicTableConfigChangeCollection,
)
