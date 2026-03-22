"""Functional tests for custom naming macros in dbt-hologres.

These tests verify that custom naming macros work correctly,
including:
- Custom schema name generation
- Default schema name fallback
- Model names with valid special characters

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_custom_naming.py
"""
import pytest

from dbt.tests.util import run_dbt


class TestCustomSchemaName:
    """Tests for hologres__generate_schema_name macro with custom schema."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with custom schema configuration."""
        return {
            "custom_schema_model.sql": """
{{ config(
    materialized='table',
    schema='custom_schema'
) }}

select 1 as id, 'test' as name
""",
        }

    def test_custom_schema_name_creates_table(self, project):
        """Test that model with custom schema is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDefaultSchemaName:
    """Tests for default schema name when not specified."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model without custom schema (uses target.schema)."""
        return {
            "default_schema_model.sql": """
{{ config(materialized='table') }}

select 1 as id, 'test' as name
""",
        }

    def test_default_schema_name_creates_table(self, project):
        """Test that model uses target.schema when no custom schema."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestModelNameWithUnderscore:
    """Tests for model names containing underscores."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with underscores in names."""
        return {
            "model_with_underscore.sql": """
{{ config(materialized='table') }}

select 1 as id
""",
            "another_underscore_model.sql": """
{{ config(materialized='table') }}

select 2 as id
""",
        }

    def test_underscore_in_name_creates_tables(self, project):
        """Test that underscores in model names work correctly."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestModelNameWithNumbers:
    """Tests for model names containing numbers."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with numbers in names."""
        return {
            "model_v1.sql": """
{{ config(materialized='table') }}

select 1 as version
""",
            "model_2024.sql": """
{{ config(materialized='table') }}

select 2024 as year
""",
        }

    def test_numbers_in_name_creates_tables(self, project):
        """Test that numbers in model names work correctly."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestMultipleCustomSchemas:
    """Tests for multiple models with different custom schemas."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with different custom schemas."""
        return {
            "staging_users.sql": """
{{ config(
    materialized='table',
    schema='staging'
) }}

select 1 as user_id, 'user1' as name
""",
            "marts_orders.sql": """
{{ config(
    materialized='table',
    schema='marts'
) }}

select 1 as order_id, 100 as amount
""",
        }

    def test_multiple_custom_schemas(self, project):
        """Test that models with different schemas are created correctly."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestSchemaNameWithUnderscore:
    """Tests for custom schema names with underscores."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with schema name containing underscore."""
        return {
            "model_with_schema_underscore.sql": """
{{ config(
    materialized='table',
    schema='raw_data'
) }}

select 1 as id
""",
        }

    def test_schema_name_with_underscore(self, project):
        """Test that schema names with underscores work correctly."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestMixedSchemaModels:
    """Tests for mix of models with and without custom schema."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with mixed schema configurations."""
        return {
            "default_model.sql": """
{{ config(materialized='table') }}

select 'default' as schema_type
""",
            "custom_model.sql": """
{{ config(
    materialized='table',
    schema='analytics'
) }}

select 'custom' as schema_type
""",
        }

    def test_mixed_schema_models(self, project):
        """Test that mixed schema configurations work together."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestViewWithCustomSchema:
    """Tests for view materialization with custom schema."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define view with custom schema."""
        return {
            "source_table.sql": """
{{ config(materialized='table') }}

select i as id, 'item_' || i as name
from generate_series(1, 10) as s(i)
""",
            "report_view.sql": """
{{ config(
    materialized='view',
    schema='reporting'
) }}

select id, upper(name) as name_upper
from {{ ref('source_table') }}
""",
        }

    def test_view_with_custom_schema(self, project):
        """Test that views can be created in custom schema."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestIncrementalWithCustomSchema:
    """Tests for incremental model with custom schema."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with custom schema."""
        return {
            "incremental_custom.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='incremental_data'
) }}

select i as id, 'value_' || i as name
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
where i > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
""",
        }

    def test_incremental_with_custom_schema(self, project):
        """Test that incremental models work with custom schema."""
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run (incremental)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
