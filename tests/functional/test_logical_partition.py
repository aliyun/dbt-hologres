"""Functional tests for logical partition in dbt-hologres.

Logical partition is a Hologres-specific feature that allows partitioning
tables by date or other columns for better query performance and data management.

These tests verify:
- Single partition key configuration
- Multiple partition keys configuration
- Partition key with various data types

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_logical_partition.py
"""
import pytest

from dbt.tests.util import run_dbt

from tests.functional.fixtures import (
    models__logical_partition_single,
    models__logical_partition_multiple,
)


class TestLogicalPartitionSingleKey:
    """Tests for logical partition with a single partition key."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with single partition key."""
        return {
            "single_partition.sql": models__logical_partition_single,
        }

    def test_single_partition_creates_table(self, project):
        """Test that a table with single partition key is created."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_single_partition_recreates(self, project):
        """Test that single partition table can be recreated."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionMultipleKeys:
    """Tests for logical partition with multiple partition keys."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with multiple partition keys."""
        return {
            "multi_partition.sql": models__logical_partition_multiple,
        }

    def test_multiple_partition_keys(self, project):
        """Test that a table with multiple partition keys is created."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionWithDate:
    """Tests for logical partition with date column."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model partitioned by date."""
        # Note: Hologres doesn't support random() function
        return {
            "date_partition.sql": """
{{ config(
    materialized='table',
    logical_partition_key='event_date'
) }}

select
    current_date - (i || ' days')::interval as event_date,
    i as event_id,
    'event_' || i as event_name,
    (i % 1000) as value
from generate_series(0, 29) as s(i)
""",
        }

    def test_date_partition(self, project):
        """Test table partitioned by date column."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionWithString:
    """Tests for logical partition with string column."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model partitioned by string column."""
        return {
            "string_partition.sql": """
{{ config(
    materialized='table',
    logical_partition_key='region'
) }}

select
    case
        when i % 3 = 0 then 'North'
        when i % 3 = 1 then 'South'
        else 'East'
    end as region,
    i as id,
    'data_' || i as name
from generate_series(1, 30) as s(i)
""",
        }

    def test_string_partition(self, project):
        """Test table partitioned by string column."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionWithYearMonth:
    """Tests for logical partition with year and month columns."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model partitioned by year and month."""
        return {
            "year_month_partition.sql": """
{{ config(
    materialized='table',
    logical_partition_key='year,month'
) }}

select
    extract(year from d)::text as year,
    extract(month from d)::text as month,
    row_number() over () as id,
    'data' as value
from (
    select current_date - (i || ' days')::interval as d
    from generate_series(0, 90) as s(i)
) t
""",
        }

    def test_year_month_partition(self, project):
        """Test table partitioned by year and month."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionWithSeed:
    """Tests for logical partition table referencing seed data."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for partition tests."""
        return {
            "partition_source.csv": """event_date,event_id,event_name
2024-01-01,1,Event_A
2024-01-02,2,Event_B
2024-01-03,3,Event_C
2024-02-01,4,Event_D
2024-02-02,5,Event_E
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define partition model from seed."""
        return {
            "partition_from_seed.sql": """
{{ config(
    materialized='table',
    logical_partition_key='event_date'
) }}

select
    event_date::date as event_date,
    event_id,
    event_name
from {{ ref('partition_source') }}
""",
        }

    def test_partition_from_seed(self, project):
        """Test logical partition table created from seed data."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionWithIncremental:
    """Tests for incremental model with logical partition.

    Note: Hologres incremental materialization does not support logical_partition_key.
    Logical partition tables should use materialized='table' instead.
    This test verifies basic incremental behavior without partitioning.
    """

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model without logical partition."""
        return {
            "incremental_partition.sql": """
{{ config(
    materialized='incremental'
) }}

select
    ds,
    id,
    name
from (
    select
        current_date - (i || ' days')::interval as ds,
        i as id,
        'data_' || i as name
    from generate_series(0, 6) as s(i)

    {% if is_incremental() %}
    where current_date - (i || ' days')::interval > (select max(ds) from {{ this }})
    {% endif %}
) t
""",
        }

    def test_incremental_with_partition(self, project):
        """Test incremental model without partition configuration."""
        # First run
        run_dbt(["run"])

        # Second run should add new data
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionCombinedWithIndex:
    """Tests for logical partition combined with indexes."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with partition and indexes."""
        # Note: Hologres doesn't support random() function
        return {
            "partition_with_index.sql": """
{{ config(
    materialized='table',
    logical_partition_key='ds',
    indexes=[
        {'columns': ['user_id'], 'type': 'bitmap'}
    ]
) }}

select
    current_date - (i || ' days')::interval as ds,
    (i % 100) as user_id,
    i as event_id,
    'event_' || i as event_name
from generate_series(0, 29) as s(i)
""",
        }

    def test_partition_with_index(self, project):
        """Test that partition and index can be combined."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLogicalPartitionCombinedWithProperties:
    """Tests for logical partition combined with table properties."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with partition and properties."""
        # Note: Hologres doesn't support random() function
        return {
            "partition_with_properties.sql": """
{{ config(
    materialized='table',
    logical_partition_key='ds',
    orientation='column',
    distribution_key='user_id',
    lifecycle=90
) }}

select
    current_date - (i || ' days')::interval as ds,
    (i % 1000) as user_id,
    i as id,
    (i % 10000)::numeric(10,2) as amount
from generate_series(0, 29) as s(i)
""",
        }

    def test_partition_with_properties(self, project):
        """Test that partition and table properties can be combined."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"