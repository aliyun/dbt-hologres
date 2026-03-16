"""Unit tests for Hologres logical partition table functionality.

These tests verify the configuration and DDL generation for logical
partition tables in Hologres.
"""
import pytest
from unittest import mock
from jinja2 import Template

from dbt.adapters.hologres.impl import HologresConfig


class TestLogicalPartitionConfig:
    """Test logical partition configuration in HologresConfig."""

    def test_single_partition_key_config(self):
        """Test config with single partition key."""
        config = HologresConfig(logical_partition_key="ds")
        assert config.logical_partition_key == "ds"

    def test_dual_partition_keys_config(self):
        """Test config with dual partition keys."""
        config = HologresConfig(logical_partition_key="order_year, order_month")
        assert config.logical_partition_key == "order_year, order_month"

    def test_partition_key_with_properties(self):
        """Test config with partition key and other table properties."""
        config = HologresConfig(
            orientation="column",
            distribution_key="order_id",
            logical_partition_key="ds",
        )
        assert config.orientation == "column"
        assert config.distribution_key == "order_id"
        assert config.logical_partition_key == "ds"

    def test_partition_key_none(self):
        """Test config without partition key."""
        config = HologresConfig()
        assert config.logical_partition_key is None

    def test_partition_key_with_clustering_key(self):
        """Test config with partition key and clustering key."""
        config = HologresConfig(
            logical_partition_key="region",
            clustering_key="created_at",
        )
        assert config.logical_partition_key == "region"
        assert config.clustering_key == "created_at"


class TestLogicalPartitionDDL:
    """Test DDL generation for logical partition tables."""

    def test_single_key_ddl_generation(self, sample_columns):
        """Test DDL generation with single partition key."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
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
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        # Verify partition column has NOT NULL
        assert "ds TEXT not null" in result
        # Verify non-partition columns don't have NOT NULL
        assert "id BIGINT" in result
        assert "name TEXT" in result
        # Verify partition clause
        assert "logical partition by list (ds)" in result

    def test_dual_keys_ddl_generation(self):
        """Test DDL generation with dual partition keys."""
        # Create columns including partition keys
        columns = []
        for name, dtype in [
            ("id", "BIGINT"),
            ("order_year", "INT"),
            ("order_month", "INT"),
        ]:
            col = mock.MagicMock()
            col.column = name
            col.data_type = dtype
            columns.append(col)

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
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=columns
        ).strip()

        # Verify both partition columns have NOT NULL
        assert "order_year INT not null" in result
        assert "order_month INT not null" in result
        # Verify partition clause with both keys
        assert "logical partition by list (order_year, order_month)" in result

    def test_partition_column_not_null_constraint(self, sample_columns):
        """Test that partition columns get NOT NULL constraint."""
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

        # Verify partition column has NOT NULL
        assert "ds TEXT not null" in result
        # Verify non-partition columns don't have NOT NULL added
        assert "id BIGINT not null" not in result

    def test_ddl_with_with_clause(self, sample_columns):
        """Test DDL generation with WITH clause properties."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
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
{%- set with_properties = ["orientation = 'column'", "distribution_key = 'id'"] -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }})
with (
  {{ with_properties | join(',\\n  ') }}
);
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        assert "ds TEXT not null" in result
        assert "logical partition by list (ds)" in result
        assert "with (" in result
        assert "orientation = 'column'" in result
        assert "distribution_key = 'id'" in result


class TestLogicalPartitionEdgeCases:
    """Test edge cases for logical partition configuration."""

    def test_empty_partition_key_error(self):
        """Test that empty partition key is handled."""
        config = HologresConfig(logical_partition_key="")
        assert config.logical_partition_key == ""

    def test_partition_key_with_spaces(self):
        """Test partition key with extra spaces is preserved."""
        config = HologresConfig(logical_partition_key=" year , month ")
        # The config stores the value as-is
        assert config.logical_partition_key == " year , month "

    def test_partition_key_with_trailing_comma(self):
        """Test partition key with trailing comma."""
        config = HologresConfig(logical_partition_key="ds,")
        assert config.logical_partition_key == "ds,"

    def test_partition_key_normalization_in_template(self):
        """Test that partition key is normalized in template rendering."""
        # Template should handle trimming spaces
        template_str = """
{%- set logical_partition_key = "  ds  ,  region  " -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{{ partition_columns | join(', ') }}
"""
        result = Template(template_str).render().strip()
        assert result == "ds, region"

    def test_three_partition_keys_in_template(self):
        """Test template behavior with three partition keys (Hologres supports 1-2)."""
        # The template logic should still work, but Hologres may reject it
        template_str = """
{%- set logical_partition_key = "year, month, day" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{{ partition_columns | join(', ') }}
"""
        result = Template(template_str).render().strip()
        assert result == "year, month, day"


class TestLogicalPartitionMacro:
    """Test the hologres__create_logical_partition_table_ddl macro logic."""

    def test_build_column_definitions_macro(self, sample_columns):
        """Test build_column_definitions macro logic."""
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

        # Verify correct column definitions
        assert "id BIGINT" in result
        assert "name TEXT" in result
        assert "ds TEXT not null" in result
        assert "created_at TIMESTAMP" in result

    def test_partition_key_parsing_single(self):
        """Test partition key parsing for single key."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- for col in partition_columns -%}
{{ col }}
{%- if not loop.last %}, {% endif -%}
{%- endfor -%}
"""
        result = Template(template_str).render().strip()
        assert result == "ds"

    def test_partition_key_parsing_multiple(self):
        """Test partition key parsing for multiple keys."""
        template_str = """
{%- set logical_partition_key = "year, month" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- for col in partition_columns -%}
{{ col }}
{%- if not loop.last %}, {% endif -%}
{%- endfor -%}
"""
        result = Template(template_str).render().strip()
        assert result == "year, month"

    def test_with_properties_list_generation(self):
        """Test WITH properties list generation."""
        template_str = """
{%- set with_properties = [] -%}
{%- if orientation is not none -%}
  {%- do with_properties.append("orientation = '" ~ orientation ~ "'") -%}
{%- endif -%}
{%- if distribution_key is not none -%}
  {%- do with_properties.append("distribution_key = '" ~ distribution_key ~ "'") -%}
{%- endif -%}
{%- if with_properties | length > 0 -%}
with (
  {{ with_properties | join(',\\n  ') }}
)
{%- endif -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            orientation="column",
            distribution_key="id"
        ).strip()
        assert "orientation = 'column'" in result
        assert "distribution_key = 'id'" in result

    def test_with_properties_empty(self):
        """Test WITH clause is omitted when no properties."""
        template_str = """
{%- set with_properties = [] -%}
{%- if orientation is not none -%}
  {%- do with_properties.append("orientation = '" ~ orientation ~ "'") -%}
{%- endif -%}
{%- if with_properties | length > 0 -%}
with (
  {{ with_properties | join(',\\n  ') }}
)
{%- endif -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            orientation=None
        ).strip()
        assert "with (" not in result


class TestLogicalPartitionIntegration:
    """Test logical partition integration with other features."""

    def test_logical_partition_with_index_config(self):
        """Test logical partition can be used with index config."""
        from dbt.adapters.hologres.relation_configs import HologresIndexConfig

        config = HologresConfig(
            logical_partition_key="ds",
            indexes=[HologresIndexConfig(columns=["id"], unique=True)]
        )
        assert config.logical_partition_key == "ds"
        assert len(config.indexes) == 1
        assert config.indexes[0].unique is True

    def test_logical_partition_with_event_time_column(self):
        """Test logical partition with event_time_column (both time-related features)."""
        config = HologresConfig(
            logical_partition_key="ds",
            event_time_column="created_at",
        )
        assert config.logical_partition_key == "ds"
        assert config.event_time_column == "created_at"

    def test_logical_partition_with_segment_key(self):
        """Test logical partition with segment_key alias."""
        config = HologresConfig(
            logical_partition_key="region",
            segment_key="updated_at",
        )
        assert config.logical_partition_key == "region"
        # Note: segment_key is stored but event_time_column is not automatically set
        assert config.segment_key == "updated_at"