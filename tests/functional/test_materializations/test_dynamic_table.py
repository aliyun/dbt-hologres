"""Functional tests for dynamic table materialization in dbt-hologres.

Dynamic tables are Hologres-specific materialized views with automatic
refresh capabilities. These tests verify:
- Basic dynamic table creation
- Target lag configuration
- Auto-refresh settings

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_materializations/test_dynamic_table.py
"""
import pytest

from dbt.tests.util import run_dbt

from tests.functional.fixtures import (
    seeds__source_events,
    seeds__source_items,
)


class TestDynamicTableBasic:
    """Tests for basic dynamic table materialization."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for dynamic table source."""
        return {
            "source_events.csv": seeds__source_events,
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define basic dynamic table model."""
        return {
            "dynamic_summary.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='30 minutes'
) }}

select
    user_id,
    count(*) as event_count,
    sum(amount) as total_amount
from {{ ref('source_events') }}
group by user_id
""",
        }

    def test_dynamic_table_creation(self, project):
        """Test that a dynamic table is created successfully."""
        # Load seed data first
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        # Create dynamic table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithAutoRefresh:
    """Tests for dynamic table with auto-refresh enabled."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for auto-refresh tests."""
        return {
            "metrics_data.csv": seeds__source_items,
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with auto-refresh."""
        return {
            "auto_refresh_metrics.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='5 minutes',
    auto_refresh=true
) }}

select
    category,
    count(*) as item_count,
    avg(value)::integer as avg_value
from {{ ref('metrics_data') }}
group by category
""",
        }

    def test_dynamic_table_with_auto_refresh(self, project):
        """Test dynamic table with auto-refresh configuration."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithoutAutoRefresh:
    """Tests for dynamic table without auto-refresh (manual refresh only)."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for manual refresh tests."""
        return {
            "manual_data.csv": """id,value,category
1,100,A
2,200,B
3,150,A
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table without auto-refresh."""
        return {
            "manual_refresh_table.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    auto_refresh=false
) }}

select
    category,
    sum(value) as total_value
from {{ ref('manual_data') }}
group by category
""",
        }

    def test_dynamic_table_manual_refresh(self, project):
        """Test dynamic table with manual refresh configuration."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithAggregates:
    """Tests for dynamic table with complex aggregations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data with more columns for aggregates."""
        return {
            "sales_data.csv": """date,product,quantity,revenue
2024-01-01,A,10,1000
2024-01-01,B,20,2000
2024-01-02,A,15,1500
2024-01-02,B,25,2500
2024-01-03,A,12,1200
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with complex aggregations."""
        return {
            "daily_sales_summary.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='15 minutes'
) }}

select
    date,
    count(distinct product) as product_count,
    sum(quantity) as total_quantity,
    sum(revenue) as total_revenue,
    avg(revenue)::numeric(10,2) as avg_revenue
from {{ ref('sales_data') }}
group by date
order by date
""",
        }

    def test_dynamic_table_with_aggregates(self, project):
        """Test dynamic table with complex aggregate functions."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithJoins:
    """Tests for dynamic table with joins."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define multiple seed files for join tests."""
        return {
            "users.csv": """user_id,user_name,region
1,Alice,North
2,Bob,South
3,Charlie,East
""",
            "orders.csv": """order_id,user_id,amount
1,1,100
2,1,200
3,2,150
4,3,300
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with join."""
        return {
            "user_order_summary.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='10 minutes'
) }}

select
    u.user_id,
    u.user_name,
    u.region,
    count(o.order_id) as order_count,
    coalesce(sum(o.amount), 0) as total_amount
from {{ ref('users') }} u
left join {{ ref('orders') }} o on u.user_id = o.user_id
group by u.user_id, u.user_name, u.region
""",
        }

    def test_dynamic_table_with_joins(self, project):
        """Test dynamic table with joins works correctly."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithPartition:
    """Tests for dynamic table with partition configuration."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define partitioned seed data."""
        return {
            "time_series.csv": """event_time,metric_name,metric_value
2024-01-01 00:00:00,cpu,50
2024-01-01 01:00:00,cpu,60
2024-01-01 00:00:00,memory,70
2024-01-01 01:00:00,memory,80
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with partition."""
        return {
            "partitioned_metrics.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='20 minutes',
    partition_by=['event_time']
) }}

select
    event_time,
    metric_name,
    avg(metric_value) as avg_value,
    max(metric_value) as max_value,
    min(metric_value) as min_value
from {{ ref('time_series') }}
group by event_time, metric_name
""",
        }

    def test_dynamic_table_with_partition(self, project):
        """Test dynamic table with partition configuration."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableWithIndexes:
    """Tests for dynamic table with index configuration."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for index tests."""
        return {
            "indexed_data.csv": """id,user_id,event_time,value
1,100,2024-01-01,10
2,100,2024-01-02,20
3,200,2024-01-01,15
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table with indexes."""
        return {
            "indexed_dynamic_table.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='10 minutes',
    indexes=[
        {'columns': ['user_id'], 'type': 'bitmap'}
    ]
) }}

select
    user_id,
    count(*) as event_count,
    sum(value) as total_value
from {{ ref('indexed_data') }}
group by user_id
""",
        }

    def test_dynamic_table_with_indexes(self, project):
        """Test dynamic table with index configuration."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDynamicTableRecreate:
    """Tests for dropping and recreating dynamic tables."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed for recreate tests."""
        return {
            "recreate_source.csv": """id,value
1,100
2,200
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define dynamic table for recreate tests."""
        return {
            "recreate_test.sql": """
{{ config(
    materialized='dynamic_table',
    target_lag='30 minutes'
) }}

select id, value from {{ ref('recreate_source') }}
""",
        }

    def test_dynamic_table_recreate(self, project):
        """Test that dynamic table can be dropped and recreated."""
        run_dbt(["seed"])

        # First run
        run_dbt(["run"])

        # Second run should recreate
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"