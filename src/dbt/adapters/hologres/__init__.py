from dbt.adapters.base import AdapterPlugin

from dbt.adapters.hologres.column import HologresColumn
from dbt.adapters.hologres.connections import HologresConnectionManager, HologresCredentials
from dbt.adapters.hologres.impl import HologresAdapter
from dbt.adapters.hologres.relation import HologresRelation
from dbt.adapters.hologres.local_date import LocalDate, parse_date, today
from dbt.include import hologres


Plugin = AdapterPlugin(
    adapter=HologresAdapter,  # type: ignore
    credentials=HologresCredentials,
    include_path=hologres.PACKAGE_PATH,
)
