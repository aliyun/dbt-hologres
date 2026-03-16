"""Functional tests for table materialization in dbt-hologres.

These tests verify that table materialization works correctly with
Hologres-specific configurations including:
- Basic table creation
- Indexes (bitmap, clustering keys)
- Hologres-specific properties (orientation, distribution_key, lifecycle)

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_materializations/test_table.py
"""
import pytest

from dbt.tests.util import run_dbt, check_relations_equal

from tests.functional.fixtures import (
    models__table_with_index,
    models__table_with_properties,
    models__table_with_clustering_keys,
    seeds__sample_data,
)


class TestTableMaterialization:
    """Tests for basic table materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define basic table model."""
        return {
            "basic_table.sql": """
{{ config(materialized='table') }}

select
    generate_series(1, 100) as id,
    'name_' || generate_series(1, 100) as name,
    random() * 1000 as value
""",
        }

    def test_table_creates_successfully(self, project):
        """Test that a basic table is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_table_is_recreatable(self, project):
        """Test that running dbt run twice recreates the table."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithIndexes:
    """Tests for table materialization with Hologres indexes."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with index configurations."""
        return {
            "indexed_table.sql": models__table_with_index,
        }

    def test_table_with_bitmap_index(self, project):
        """Test creating a table with bitmap indexes."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithClusteringKeys:
    """Tests for table with clustering key configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with clustering keys."""
        return {
            "clustered_table.sql": models__table_with_clustering_keys,
        }

    def test_table_with_clustering_keys(self, project):
        """Test creating a table with clustering keys."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithProperties:
    """Tests for table with Hologres-specific properties."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with Hologres properties."""
        return {
            "property_table.sql": models__table_with_properties,
        }

    def test_table_with_orientation(self, project):
        """Test creating a table with column orientation."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithDistributionKey:
    """Tests for table with distribution key configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with distribution key."""
        return {
            "distributed_table.sql": """
{{ config(
    materialized='table',
    distribution_key='user_id'
) }}

select
    (random() * 1000)::int as user_id,
    (random() * 100)::int as product_id,
    random() * 1000 as amount
from generate_series(1, 1000)
""",
        }

    def test_table_with_distribution_key(self, project):
        """Test creating a table with distribution key."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithLifecycle:
    """Tests for table with lifecycle property."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with lifecycle configuration."""
        return {
            "lifecycle_table.sql": """
{{ config(
    materialized='table',
    lifecycle=30
) }}

select
    generate_series(1, 100) as id,
    current_timestamp as created_at
""",
        }

    def test_table_with_lifecycle(self, project):
        """Test creating a table with lifecycle property."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithTableGroup:
    """Tests for table with table group property."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with table group configuration."""
        return {
            "grouped_table.sql": """
{{ config(
    materialized='table',
    table_group='test_group'
) }}

select
    generate_series(1, 100) as id,
    'test' as name
""",
        }

    def test_table_with_table_group(self, project):
        """Test creating a table with table group property."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableWithMultipleConfigurations:
    """Tests for table with multiple Hologres configurations combined."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with multiple configurations."""
        return {
            "complex_table.sql": """
{{ config(
    materialized='table',
    orientation='column',
    distribution_key='user_id',
    clustering_keys=['event_time'],
    indexes=[
        {'columns': ['user_id'], 'type': 'bitmap'}
    ],
    lifecycle=90
) }}

select
    current_timestamp - (i || ' days')::interval as event_time,
    (random() * 1000)::int as user_id,
    i as id,
    'data_' || i as name
from generate_series(1, 100) as s(i)
""",
        }

    def test_table_with_multiple_configs(self, project):
        """Test creating a table with multiple configurations."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableDropAndRecreate:
    """Tests for dropping and recreating tables."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model for drop/recreate tests."""
        return {
            "drop_test.sql": """
{{ config(materialized='table') }}

select 1 as id, 'first' as version
""",
        }

    def test_table_drop_recreate(self, project):
        """Test that table is properly dropped and recreated."""
        # First run
        run_dbt(["run"])

        # Update the model (simulated by running again)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"