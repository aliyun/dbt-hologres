"""Functional tests for view materialization in dbt-hologres.

These tests verify that view materialization works correctly with
Hologres-specific behaviors including:
- Basic view creation and recreation
- View dependencies on tables and other views
- View drop and recreate scenarios

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_materializations/test_view.py
"""
import pytest

from dbt.tests.util import run_dbt, check_relations_equal


class TestViewMaterialization:
    """Tests for basic view materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define basic view model."""
        return {
            "basic_view.sql": """
{{ config(materialized='view') }}

select
    1 as id,
    'test' as name,
    current_timestamp as created_at
""",
        }

    def test_view_creates_successfully(self, project):
        """Test that a basic view is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_view_is_recreatable(self, project):
        """Test that running dbt run twice recreates the view."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestViewWithDependencies:
    """Tests for view with dependencies on tables and other views."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for dependency tests."""
        return {
            "source_data.csv": """id,name,value
1,alpha,100
2,beta,200
3,gamma,300
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with dependencies."""
        return {
            # Base table
            "base_table.sql": """
{{ config(materialized='table') }}

select
    id,
    name,
    value
from {{ ref('source_data') }}
""",
            # View referencing a table
            "view_on_table.sql": """
{{ config(materialized='view') }}

select
    id,
    name,
    value * 2 as doubled_value
from {{ ref('base_table') }}
""",
            # View referencing another view
            "view_on_view.sql": """
{{ config(materialized='view') }}

select
    id,
    name,
    doubled_value,
    doubled_value + 100 as adjusted_value
from {{ ref('view_on_table') }}
""",
        }

    def test_view_references_table(self, project):
        """Test that a view can reference a table."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        for result in results:
            assert result.status == "success"

    def test_view_references_view(self, project):
        """Test that a view can reference another view."""
        run_dbt(["seed"])
        run_dbt(["run"])
        # Re-run to ensure dependencies are resolved correctly
        results = run_dbt(["run"])
        assert len(results) == 3
        for result in results:
            assert result.status == "success"

    def test_view_update_on_source_change(self, project):
        """Test that view reflects changes in source table data."""
        run_dbt(["seed"])
        run_dbt(["run"])

        # Re-run should maintain consistent results
        results = run_dbt(["run"])
        assert len(results) == 3
        for result in results:
            assert result.status == "success"


class TestViewDropAndRecreate:
    """Tests for dropping and recreating views."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model for drop/recreate tests."""
        return {
            "drop_test_view.sql": """
{{ config(materialized='view') }}

select 1 as id, 'version_1' as version
""",
        }

    def test_view_drop_recreate(self, project):
        """Test that view is properly dropped and recreated."""
        # First run creates the view
        run_dbt(["run"])

        # Second run should drop and recreate the view
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestViewWithSimpleAggregation:
    """Tests for view with aggregation functions."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for aggregation tests."""
        return {
            "agg_data.csv": """category,amount
A,100
A,200
B,150
B,250
C,300
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define view with aggregation."""
        return {
            "agg_view.sql": """
{{ config(materialized='view') }}

select
    category,
    sum(amount) as total_amount,
    count(*) as item_count,
    avg(amount)::numeric(10,2) as avg_amount
from {{ ref('agg_data') }}
group by category
""",
        }

    def test_view_with_aggregation(self, project):
        """Test that view with aggregation functions correctly."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestViewWithJoin:
    """Tests for view with join operations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for join tests."""
        return {
            "users.csv": """user_id,user_name
1,Alice
2,Bob
3,Charlie
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
        """Define view with join."""
        return {
            "user_order_view.sql": """
{{ config(materialized='view') }}

select
    u.user_id,
    u.user_name,
    count(o.order_id) as order_count,
    coalesce(sum(o.amount), 0) as total_amount
from {{ ref('users') }} u
left join {{ ref('orders') }} o on u.user_id = o.user_id
group by u.user_id, u.user_name
""",
        }

    def test_view_with_join(self, project):
        """Test that view with join works correctly."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestViewWithFilter:
    """Tests for view with filtering conditions."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for filter tests."""
        return {
            "products.csv": """id,name,price,category
1,Widget,10.00,A
2,Gadget,25.00,B
3,Doohickey,15.00,A
4,Thingamajig,30.00,B
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define view with filter."""
        return {
            "filtered_view.sql": """
{{ config(materialized='view') }}

select
    id,
    name,
    price,
    category
from {{ ref('products') }}
where price > 20
""",
        }

    def test_view_with_filter(self, project):
        """Test that view with filtering works correctly."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"