from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, List, Optional, Set, Union

from dbt.adapters.base import AdapterConfig, ConstraintSupport, available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.exceptions import (
    CrossDbReferenceProhibitedError,
    IndexConfigError,
    IndexConfigNotDictError,
    UnexpectedDbReferenceError,
)
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hologres.column import HologresColumn
from dbt.adapters.hologres.connections import HologresConnectionManager
from dbt.adapters.hologres.relation import HologresRelation
from dbt.adapters.hologres.relation_configs import HologresIndexConfig
from dbt.adapters.hologres.local_date import LocalDate, parse_date as _parse_date, today as _today


GET_RELATIONS_MACRO_NAME = "hologres__get_relations"


@dataclass
class HologresConfig(AdapterConfig):
    indexes: Optional[List[HologresIndexConfig]] = None
    # Table property configurations for Hologres 2.1+ WITH clause
    orientation: Optional[str] = None
    distribution_key: Optional[str] = None
    clustering_key: Optional[str] = None
    event_time_column: Optional[str] = None
    segment_key: Optional[str] = None  # Alias for event_time_column
    bitmap_columns: Optional[str] = None
    dictionary_encoding_columns: Optional[str] = None
    # Logical partition configuration (supports 1-2 columns)
    # Example: 'ds' for single key, 'yy,mm' for two keys
    logical_partition_key: Optional[str] = None


class HologresAdapter(SQLAdapter):
    Relation = HologresRelation
    ConnectionManager = HologresConnectionManager
    Column = HologresColumn

    AdapterSpecificConfigs = HologresConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    CATALOG_BY_RELATION_SUPPORT = True

    _capabilities: CapabilityDict = CapabilityDict(
        {Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full)}
    )

    @classmethod
    def date_function(cls):
        return "now()"

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise UnexpectedDbReferenceError(self.type(), database, expected)
        # return an empty string on success so macros can call this
        return ""

    @available
    def parse_index(self, raw_index: Any) -> Optional[HologresIndexConfig]:
        return HologresIndexConfig.parse(raw_index)

    @available
    def parse_date(self, date_input: Union[str, date, datetime, None] = None) -> LocalDate:
        """
        Parse a date string or object into a LocalDate instance for chainable date operations.
        
        This function can be called in Jinja2 templates as: {{ adapter.parse_date('2024-01-15') }}
        Or through the parse_date macro as: {{ parse_date('2024-01-15') }}
        
        Args:
            date_input: Date string (YYYY-MM-DD), date object, datetime object, or None (today)
            
        Returns:
            LocalDate instance supporting chainable date operations like:
            - sub_days(n), sub_months(n), sub_years(n)
            - add_days(n), add_months(n), add_years(n)
            - start_of_month(), end_of_month()
            - start_of_quarter(), end_of_quarter()
            - start_of_year(), end_of_year()
            
        Example:
            {%- set ds = parse_date('2024-01-15') -%}
            {%- set start_date = ds.sub_months(2).start_of_month() -%}
            -- start_date will be '2023-11-01'
        """
        return _parse_date(date_input)

    @available
    def today(self) -> LocalDate:
        """
        Get today's date as a LocalDate instance.
        
        Returns:
            LocalDate instance for today
            
        Example:
            {%- set current = adapter.today() -%}
            {%- set last_month = current.sub_months(1).start_of_month() -%}
        """
        return _today()

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

        for dep_schema, dep_name, refed_schema, refed_name in table:
            dependent = self.Relation.create(
                database=database, schema=dep_schema, identifier=dep_name
            )
            referenced = self.Relation.create(
                database=database, schema=refed_schema, identifier=refed_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(referenced, dependent)

    def _get_catalog_schemas(self, manifest):
        # hologres only allow one database (the main one)
        schema_search_map = super()._get_catalog_schemas(manifest)
        try:
            return schema_search_map.flatten()
        except DbtRuntimeError as exc:
            raise CrossDbReferenceProhibitedError(self.type(), exc.msg)

    def _link_cached_relations(self, manifest) -> None:
        schemas: Set[str] = set()
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())  # type: ignore

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest, cache_schemas=None):
        super()._relations_cache_for_schemas(manifest, cache_schemas)
        self._link_cached_relations(manifest)

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number} {interval}'"

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "merge", "microbatch"]

    def debug_query(self):
        self.execute("select 1 as id")
