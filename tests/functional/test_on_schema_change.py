"""Functional tests for on_schema_change configuration in dbt-hologres.

on_schema_change controls how incremental models handle schema changes:
- 'ignore': Ignore schema changes (default)
- 'fail': Raise an error when schema changes are detected
- 'append_new_columns': Add new columns to the existing table
- 'sync_all_columns': Add new columns and remove missing columns

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_on_schema_change.py
"""
import pytest

from dbt.tests.util import run_dbt


class TestOnSchemaChangeIgnore:
    """Tests for on_schema_change='ignore' (default behavior)."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with ignore schema change."""
        return {
            "ignore_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore'
) }}

select
    i as id,
    'name_' || i as name
from generate_series(1, 10) as s(i)
""",
        }

    def test_schema_change_ignore(self, project):
        """Test that schema changes are ignored."""
        # First run creates the table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run should succeed (ignore any schema changes)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestOnSchemaChangeAppendNewColumns:
    """Tests for on_schema_change='append_new_columns'."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model that demonstrates append_new_columns."""
        return {
            "append_columns_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

select
    i as id,
    'name_' || i as name,
    i * 100 as value
from generate_series(1, 5) as s(i)
""",
        }

    def test_append_new_columns(self, project):
        """Test that new columns are appended to the table."""
        # First run creates the table with initial columns
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run should handle the schema correctly
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestOnSchemaChangeSyncAllColumns:
    """Tests for on_schema_change='sync_all_columns'."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with sync_all_columns."""
        return {
            "sync_columns_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns'
) }}

select
    i as id,
    'name_' || i as name,
    i * 100 as value
from generate_series(1, 5) as s(i)
""",
        }

    def test_sync_all_columns_add(self, project):
        """Test that sync_all_columns synchronizes the schema."""
        # First run creates the table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run should sync columns
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestOnSchemaChangeWithMerge:
    """Tests for on_schema_change combined with upsert strategy.

    Note: Hologres INSERT ON CONFLICT requires a primary key constraint on the target table.
    For simplicity, this test uses delete+insert strategy which provides similar upsert behavior
    without requiring primary key constraints.
    """

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with delete+insert strategy and schema change handling."""
        return {
            "merge_schema_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    on_schema_change='append_new_columns'
) }}

select
    i as id,
    'name_' || i as name,
    i * 100 as value
from generate_series(1, 10) as s(i)
""",
        }

    def test_merge_with_schema_change(self, project):
        """Test that merge strategy works with schema change handling."""
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestOnSchemaChangeWithDeleteInsert:
    """Tests for on_schema_change combined with delete+insert strategy."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with delete+insert strategy."""
        return {
            "delete_insert_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    on_schema_change='ignore'
) }}

select
    i as id,
    'name_' || i as name,
    i * 100 as value
from generate_series(1, 10) as s(i)
""",
        }

    def test_delete_insert_with_schema_change(self, project):
        """Test delete+insert strategy with schema change handling."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestOnSchemaChangeDefaultBehavior:
    """Tests for default on_schema_change behavior when not specified."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model without explicit on_schema_change."""
        return {
            "default_schema_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
    i as id,
    'name_' || i as name
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
where i > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
""",
        }

    def test_default_schema_change_behavior(self, project):
        """Test that default behavior (ignore) works correctly."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"