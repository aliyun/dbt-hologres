"""Functional tests for sql_header configuration in dbt-hologres.

sql_header allows setting GUC (Grand Unified Configuration) parameters
before executing CREATE statements. This is useful for:
- Setting timezone for session
- Configuring computing resources (serverless)
- Setting other Hologres-specific parameters

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_sql_header.py
"""
import pytest

from dbt.tests.util import run_dbt


class TestSqlHeaderBasic:
    """Tests for basic sql_header functionality."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with basic sql_header."""
        return {
            "timezone_view.sql": """
{{ config(
    materialized='view',
    sql_header="SET TIME ZONE 'UTC';"
) }}

select
    1 as id,
    current_timestamp as created_at
""",
        }

    def test_sql_header_set_timezone(self, project):
        """Test that sql_header sets timezone for view creation."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderMultipleStatements:
    """Tests for sql_header with multiple GUC statements."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with multiple sql_header statements."""
        return {
            "multi_header_table.sql": """
{{ config(
    materialized='table',
    sql_header="SET TIME ZONE 'UTC'; SET enable_seqscan = on;"
) }}

select
    generate_series(1, 10) as id,
    'test' as name
""",
        }

    def test_sql_header_multiple_statements(self, project):
        """Test that multiple GUC statements in sql_header work correctly."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderWithTable:
    """Tests for sql_header combined with table materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table model with sql_header for serverless computing."""
        return {
            "serverless_table.sql": """
{{ config(
    materialized='table',
    sql_header="SET hg_computing_resource = 'serverless';"
) }}

select
    generate_series(1, 100) as id,
    'data_' || generate_series(1, 100) as name,
    (i % 100)::bigint as value
from generate_series(1, 100) as s(i)
""",
        }

    def test_table_with_sql_header_serverless(self, project):
        """Test that sql_header configures serverless computing for table."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_table_with_sql_header_persists(self, project):
        """Test that configuration persists across multiple runs."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderWithView:
    """Tests for sql_header combined with view materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define view model with sql_header."""
        return {
            "header_view.sql": """
{{ config(
    materialized='view',
    sql_header="SET TIME ZONE 'Asia/Shanghai';"
) }}

select
    1 as id,
    current_timestamp as local_time
""",
        }

    def test_view_with_sql_header(self, project):
        """Test that sql_header works with view materialization."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderWithIncremental:
    """Tests for sql_header combined with incremental materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with sql_header."""
        return {
            "incremental_with_header.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    sql_header="SET hg_computing_resource = 'serverless';"
) }}

select
    i as id,
    'value_' || i as name
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
where i > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
""",
        }

    def test_incremental_with_sql_header(self, project):
        """Test that sql_header works with incremental models."""
        # First run creates the table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run tests incremental behavior
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderWithDynamicTable:
    """Tests for sql_header combined with dynamic table materialization."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for dynamic table."""
        return {
            "source_events.csv": """user_id,amount,event_time
1,100.50,2024-01-01 10:00:00
2,250.00,2024-01-01 11:00:00
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with sql_header."""
        return {
            "dynamic_with_header.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='30 minutes',
    sql_header="SET TIME ZONE 'UTC';"
) }}

select
    user_id,
    sum(amount) as total_amount
from {{ ref('source_events') }}
group by user_id
""",
        }

    def test_dynamic_table_with_sql_header(self, project):
        """Test that sql_header works with dynamic table."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSqlHeaderEmpty:
    """Tests for handling empty or None sql_header."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model without sql_header (default behavior)."""
        return {
            "no_header_table.sql": """
{{ config(materialized='table') }}

select
    1 as id,
    'test' as name
""",
        }

    def test_table_without_sql_header(self, project):
        """Test that tables work correctly without sql_header."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"