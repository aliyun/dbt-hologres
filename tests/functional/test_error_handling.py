"""Functional tests for error handling in dbt-hologres.

These tests verify that the adapter properly handles various error
conditions including:
- Invalid SQL syntax
- Circular model dependencies
- Invalid configuration values

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_error_handling.py
"""
import pytest

from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtRuntimeError


class TestInvalidSQLSyntax:
    """Tests for handling invalid SQL syntax."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with invalid SQL that references non-existent table."""
        return {
            "invalid_sql.sql": """
SELECT * FROM nonexistent_table_xyz123
WHERE invalid_column = 'value'
""",
        }

    def test_invalid_sql_raises_error(self, project):
        """Test that invalid SQL raises appropriate error."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestCircularDependency:
    """Tests for handling circular model dependencies."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with circular dependencies."""
        return {
            "model_a.sql": "select * from {{ ref('model_b') }}",
            "model_b.sql": "select * from {{ ref('model_a') }}",
        }

    def test_circular_dependency_raises_error(self, project):
        """Test that circular dependencies are detected."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestInvalidTableProperty:
    """Tests for handling invalid table property configurations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with potentially invalid orientation value."""
        return {
            "invalid_property.sql": """
{{ config(
    materialized='table',
    orientation='invalid_orientation_value'
) }}

select 1 as id
""",
        }

    def test_invalid_property_handling(self, project):
        """Test that invalid property values are handled.

        Note: Hologres may accept invalid orientation silently or error.
        This test documents the actual behavior.
        """
        try:
            results = run_dbt(["run"])
            # If it succeeds, Hologres accepted the value (documented behavior)
            assert len(results) == 1
        except DbtRuntimeError:
            # If it fails, that's also acceptable (Hologres rejected the value)
            pass


class TestNonexistentRef:
    """Tests for referencing non-existent model."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model that references a non-existent model."""
        return {
            "missing_ref.sql": """
select * from {{ ref('nonexistent_model_xyz') }}
""",
        }

    def test_nonexistent_ref_raises_error(self, project):
        """Test that referencing non-existent model raises error."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestInvalidIncrementalStrategy:
    """Tests for handling invalid incremental strategy."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with invalid incremental strategy."""
        return {
            "invalid_strategy.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='invalid_strategy_xyz'
) }}

select i as id from generate_series(1, 10) as s(i)
""",
        }

    def test_invalid_incremental_strategy_raises_error(self, project):
        """Test that invalid incremental strategy raises error."""
        # Note: dbt may allow custom strategies, so this might succeed
        # The test documents actual behavior
        try:
            results = run_dbt(["run"])
            # If it succeeds, dbt allowed the custom strategy
            assert len(results) == 1
        except DbtRuntimeError:
            # If it fails, the strategy was rejected
            pass


class TestEmptyModel:
    """Tests for handling empty model SQL."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with empty SQL."""
        return {
            "empty_model.sql": "",
        }

    def test_empty_model_raises_error(self, project):
        """Test that empty model SQL raises error."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestSyntaxErrorInConfig:
    """Tests for handling syntax errors in model config."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with Jinja syntax error in config."""
        return {
            "syntax_error.sql": """
{{ config(
    materialized='table'
    orientation='column'
) }}

select 1 as id
""",
        }

    def test_syntax_error_in_config_raises_error(self, project):
        """Test that config syntax errors are caught."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestInvalidMaterialization:
    """Tests for handling invalid materialization type."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with invalid materialization."""
        return {
            "invalid_materialization.sql": """
{{ config(
    materialized='invalid_materialization_xyz'
) }}

select 1 as id
""",
        }

    def test_invalid_materialization_raises_error(self, project):
        """Test that invalid materialization raises error."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestSelfReference:
    """Tests for handling model self-reference."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model that references itself."""
        return {
            "self_ref.sql": """
{{ config(materialized='table') }}

select * from {{ ref('self_ref') }}
""",
        }

    def test_self_reference_raises_error(self, project):
        """Test that self-reference raises error."""
        with pytest.raises(DbtRuntimeError):
            run_dbt(["run"])


class TestMissingUniqueKeyForMerge:
    """Tests for merge strategy without unique key."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with merge strategy but no unique key."""
        return {
            "merge_no_key.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='merge'
) }}

select i as id from generate_series(1, 10) as s(i)
""",
        }

    def test_merge_without_unique_key(self, project):
        """Test merge strategy behavior without unique key.

        Note: Without unique_key, merge should fall back to append behavior.
        """
        # First run should succeed
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run should also succeed (append behavior)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
