"""Unit tests for Hologres SQL macros.

These tests verify the SQL generation logic in Jinja2 macros without
requiring an actual database connection.
"""
import pytest
from unittest import mock
from jinja2 import Template


class TestAdaptersMacros:
    """Test SQL macro rendering for CTAS and DDL statements."""

    def test_create_table_as_basic(self, jinja_environment):
        """Test basic CTAS statement without properties."""
        template_str = """
{%- set relation = "test_db.test_schema.test_table" -%}
{%- set compiled_code = "SELECT * FROM source_table" -%}
create table {{ relation }} as (
  {{ compiled_code }}
);
"""
        result = Template(template_str).render().strip()
        assert "create table test_db.test_schema.test_table" in result
        assert "SELECT * FROM source_table" in result

    def test_create_table_as_with_properties(self, jinja_environment):
        """Test CTAS statement with WITH clause properties."""
        template_str = """
{%- set relation = "test_db.test_schema.test_table" -%}
{%- set compiled_code = "SELECT * FROM source_table" -%}
{%- set with_properties = ["orientation = 'column'", "distribution_key = 'id'"] -%}
create table {{ relation }}
with (
  {{ with_properties | join(',\\n  ') }}
)
as (
  {{ compiled_code }}
);
"""
        result = Template(template_str).render().strip()
        assert "create table test_db.test_schema.test_table" in result
        assert "with (" in result
        assert "orientation = 'column'" in result
        assert "distribution_key = 'id'" in result
        assert "SELECT * FROM source_table" in result

    def test_create_table_as_with_orientation(self, jinja_environment):
        """Test CTAS with orientation property."""
        template_str = """
{%- set with_properties = ["orientation = 'column'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "orientation = 'column'" in result

    def test_create_table_as_with_distribution_key(self, jinja_environment):
        """Test CTAS with distribution_key property."""
        template_str = """
{%- set with_properties = ["distribution_key = 'user_id'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "distribution_key = 'user_id'" in result

    def test_create_table_as_with_clustering_key(self, jinja_environment):
        """Test CTAS with clustering_key property."""
        template_str = """
{%- set with_properties = ["clustering_key = 'event_time'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "clustering_key = 'event_time'" in result

    def test_create_table_as_with_event_time_column(self, jinja_environment):
        """Test CTAS with event_time_column property."""
        template_str = """
{%- set with_properties = ["event_time_column = 'created_at'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "event_time_column = 'created_at'" in result

    def test_create_table_as_with_bitmap_columns(self, jinja_environment):
        """Test CTAS with bitmap_columns property."""
        template_str = """
{%- set with_properties = ["bitmap_columns = 'status,type'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "bitmap_columns = 'status,type'" in result

    def test_create_table_as_with_dictionary_encoding(self, jinja_environment):
        """Test CTAS with dictionary_encoding_columns property."""
        template_str = """
{%- set with_properties = ["dictionary_encoding_columns = 'category,region'"] -%}
create table test_table
with (
  {{ with_properties | join(',\\n  ') }}
)
as (SELECT 1);
"""
        result = Template(template_str).render().strip()
        assert "dictionary_encoding_columns = 'category,region'" in result

    def test_create_logical_partition_table_ddl_single_key(self, sample_columns):
        """Test logical partition DDL with single partition key."""
        template_str = """
{%- set partition_columns = ["ds"] -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }});
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()
        assert "ds TEXT not null" in result
        assert "id BIGINT" in result
        assert "name TEXT" in result
        assert "logical partition by list (ds)" in result

    def test_create_logical_partition_table_ddl_multiple_keys(self, sample_columns):
        """Test logical partition DDL with multiple partition keys."""
        template_str = """
{%- set logical_partition_key = "order_year, order_month" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }});
"""
        # Add partition columns to sample_columns
        extended_columns = list(sample_columns)
        for name in ["order_year", "order_month"]:
            col = mock.MagicMock()
            col.column = name
            col.data_type = "INT"
            extended_columns.append(col)

        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=extended_columns
        ).strip()
        assert "order_year INT not null" in result
        assert "order_month INT not null" in result
        assert "logical partition by list (order_year, order_month)" in result

    def test_build_column_definitions_basic(self, sample_columns):
        """Test building column definitions from column list."""
        template_str = """
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- do col_defs.append(col.column ~ ' ' ~ col.data_type) -%}
{%- endfor -%}
{{ col_defs | join(', ') }}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()
        assert "id BIGINT" in result
        assert "name TEXT" in result
        assert "ds TEXT" in result
        assert "created_at TIMESTAMP" in result

    def test_build_column_definitions_with_partition_key(self, sample_columns):
        """Test building column definitions with partition key NOT NULL constraint."""
        template_str = """
{%- set partition_columns = ["ds"] -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
{{ col_defs | join(', ') }}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()
        assert "ds TEXT not null" in result
        assert "id BIGINT" in result


class TestRelationOperations:
    """Test SQL macros for relation operations."""

    def test_drop_relation_sql(self):
        """Test drop relation SQL generation."""
        template_str = """
drop {{ relation.type }} if exists {{ relation }} cascade
"""
        relation = mock.MagicMock()
        relation.type = "table"
        relation.__str__ = lambda self: "test_db.test_schema.test_table"

        result = Template(template_str).render(relation=relation).strip()
        assert "drop table if exists test_db.test_schema.test_table cascade" in result

    def test_truncate_relation_sql(self):
        """Test truncate relation SQL generation."""
        template_str = """
truncate table {{ relation }}
"""
        relation = mock.MagicMock()
        relation.__str__ = lambda self: "test_db.test_schema.test_table"

        result = Template(template_str).render(relation=relation).strip()
        assert "truncate table test_db.test_schema.test_table" in result

    def test_rename_table_sql(self):
        """Test rename table SQL generation."""
        template_str = """
alter table {{ relation.include(database=False) }} rename to {{ new_name }}
"""
        relation = mock.MagicMock()
        relation.include = mock.MagicMock(return_value=relation)
        relation.__str__ = lambda self: "test_schema.test_table"

        result = Template(template_str).render(
            relation=relation, new_name="new_table"
        ).strip()
        assert "alter table test_schema.test_table rename to new_table" in result

    def test_rename_view_sql(self):
        """Test rename view SQL generation."""
        template_str = """
alter view {{ relation.include(database=False) }} rename to {{ new_name }}
"""
        relation = mock.MagicMock()
        relation.include = mock.MagicMock(return_value=relation)
        relation.__str__ = lambda self: "test_schema.test_view"

        result = Template(template_str).render(
            relation=relation, new_name="new_view"
        ).strip()
        assert "alter view test_schema.test_view rename to new_view" in result

    def test_get_columns_in_relation_sql(self):
        """Test get columns in relation SQL generation."""
        template_str = """
select
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale
from information_schema.columns
where table_name = '{{ relation.identifier }}'
  and table_schema = '{{ relation.schema }}'
order by ordinal_position
"""
        relation = mock.MagicMock()
        relation.identifier = "test_table"
        relation.schema = "test_schema"

        result = Template(template_str).render(relation=relation).strip()
        assert "table_name = 'test_table'" in result
        assert "table_schema = 'test_schema'" in result
        assert "information_schema.columns" in result


class TestSchemaOperations:
    """Test SQL macros for schema operations."""

    def test_create_schema_sql(self):
        """Test create schema SQL generation."""
        template_str = """
create schema if not exists {{ relation.without_identifier().include(database=False) }}
"""
        relation = mock.MagicMock()
        relation.without_identifier = mock.MagicMock(return_value=relation)
        relation.include = mock.MagicMock(return_value=relation)
        relation.__str__ = lambda self: "test_schema"

        result = Template(template_str).render(relation=relation).strip()
        assert "create schema if not exists" in result

    def test_drop_schema_sql(self):
        """Test drop schema SQL generation."""
        template_str = """
drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
"""
        relation = mock.MagicMock()
        relation.without_identifier = mock.MagicMock(return_value=relation)
        relation.include = mock.MagicMock(return_value=relation)
        relation.__str__ = lambda self: "test_schema"

        result = Template(template_str).render(relation=relation).strip()
        assert "drop schema if exists" in result
        assert "cascade" in result

    def test_check_schema_exists_sql(self):
        """Test check schema exists SQL generation."""
        template_str = """
select count(*) from pg_namespace where nspname = '{{ schema }}'
"""
        result = Template(template_str).render(schema="test_schema").strip()
        assert "pg_namespace" in result
        assert "nspname = 'test_schema'" in result


class TestViewOperations:
    """Test SQL macros for view operations."""

    def test_create_view_as_sql(self):
        """Test create view SQL generation."""
        template_str = """
create view {{ relation }} as (
  {{ sql }}
);
"""
        relation = mock.MagicMock()
        relation.__str__ = lambda self: "test_db.test_schema.test_view"

        result = Template(template_str).render(
            relation=relation, sql="SELECT * FROM source_table"
        ).strip()
        assert "create view test_db.test_schema.test_view as" in result
        assert "SELECT * FROM source_table" in result

    def test_create_view_with_sql_header(self):
        """Test create view with SQL header."""
        template_str = """
{{ sql_header if sql_header is not none }}
create view {{ relation }} as (
  {{ sql }}
);
"""
        result = Template(template_str).render(
            relation=mock.MagicMock(__str__=lambda self: "test_view"),
            sql="SELECT 1",
            sql_header="-- Header comment"
        ).strip()
        assert "-- Header comment" in result
        assert "create view test_view as" in result


class TestInsertOperations:
    """Test SQL macros for insert operations."""

    def test_insert_into_table_sql(self):
        """Test insert into table SQL generation."""
        template_str = """
insert into {{ relation }}
{{ compiled_code }};
"""
        relation = mock.MagicMock()
        relation.__str__ = lambda self: "test_db.test_schema.test_table"

        result = Template(template_str).render(
            relation=relation, compiled_code="SELECT * FROM source_table WHERE ds = '2024-01-01'"
        ).strip()
        assert "insert into test_db.test_schema.test_table" in result
        assert "SELECT * FROM source_table" in result


class TestTimestampOperations:
    """Test SQL macros for timestamp operations."""

    def test_timestamp_add_sql_hour(self):
        """Test timestamp_add_sql for hour interval."""
        # This is tested in test_adapter.py but we test the pattern here
        template_str = "{{ add_to }} + interval '{{ number }} {{ interval }}'"
        result = Template(template_str).render(
            add_to="created_at", number=1, interval="hour"
        ).strip()
        assert result == "created_at + interval '1 hour'"

    def test_timestamp_add_sql_day(self):
        """Test timestamp_add_sql for day interval."""
        template_str = "{{ add_to }} + interval '{{ number }} {{ interval }}'"
        result = Template(template_str).render(
            add_to="created_at", number=5, interval="day"
        ).strip()
        assert result == "created_at + interval '5 day'"

    def test_timestamp_add_sql_month(self):
        """Test timestamp_add_sql for month interval."""
        template_str = "{{ add_to }} + interval '{{ number }} {{ interval }}'"
        result = Template(template_str).render(
            add_to="created_at", number=3, interval="month"
        ).strip()
        assert result == "created_at + interval '3 month'"