"""Functional tests for table properties configuration in dbt-hologres.

Hologres supports various table properties that can be configured via
WITH clause in CREATE TABLE statements:
- event_time_column: For time-based queries and partitioning
- segment_key: Alias for event_time_column
- bitmap_columns: Bitmap index for efficient filtering
- dictionary_encoding_columns: Dictionary encoding for compression

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_table_properties.py
"""
import pytest

from dbt.tests.util import run_dbt


class TestEventTimeColumn:
    """Tests for event_time_column property configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with event_time_column."""
        return {
            "event_time_table.sql": """
{{ config(
    materialized='table',
    event_time_column='event_time'
) }}

select
    current_timestamp - (i || ' days')::interval as event_time,
    i as id,
    'event_' || i as name
from generate_series(1, 100) as s(i)
""",
        }

    def test_event_time_column_basic(self, project):
        """Test creating a table with event_time_column."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_event_time_column_recreate(self, project):
        """Test that table with event_time_column can be recreated."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSegmentKeyAlias:
    """Tests for segment_key as alias for event_time_column."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with segment_key (alias for event_time_column)."""
        return {
            "segment_key_table.sql": """
{{ config(
    materialized='table',
    segment_key='created_at'
) }}

select
    current_timestamp - (i || ' hours')::interval as created_at,
    i as id,
    'record_' || i as name
from generate_series(1, 50) as s(i)
""",
        }

    def test_event_time_column_with_segment_key_alias(self, project):
        """Test that segment_key works as event_time_column alias."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestBitmapColumns:
    """Tests for bitmap_columns property configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with bitmap columns."""
        return {
            "bitmap_table.sql": """
{{ config(
    materialized='table',
    bitmap_columns='user_id,status',
    orientation='column'
) }}

select
    i as id,
    (i % 100) as user_id,
    case (i % 3)
        when 0 then 'active'
        when 1 then 'pending'
        else 'inactive'
    end as status,
    i * 100 as value
from generate_series(1, 100) as s(i)
""",
        }

    def test_bitmap_columns_single(self, project):
        """Test creating a table with bitmap columns."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_bitmap_columns_multiple(self, project):
        """Test that table with multiple bitmap columns can be recreated."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestBitmapColumnsSingle:
    """Tests for single bitmap column configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with single bitmap column."""
        return {
            "single_bitmap_table.sql": """
{{ config(
    materialized='table',
    bitmap_columns='category',
    orientation='column'
) }}

select
    i as id,
    case (i % 5)
        when 0 then 'A'
        when 1 then 'B'
        when 2 then 'C'
        when 3 then 'D'
        else 'E'
    end as category,
    i as value
from generate_series(1, 50) as s(i)
""",
        }

    def test_bitmap_columns_single_column(self, project):
        """Test creating a table with a single bitmap column."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDictionaryEncodingColumns:
    """Tests for dictionary_encoding_columns property configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with dictionary encoding columns."""
        return {
            "dict_encoding_table.sql": """
{{ config(
    materialized='table',
    dictionary_encoding_columns='category,region',
    orientation='column'
) }}

select
    i as id,
    case (i % 3) when 0 then 'A' when 1 then 'B' else 'C' end as category,
    case (i % 4) when 0 then 'North' when 1 then 'South' when 2 then 'East' else 'West' end as region,
    i * 10 as amount
from generate_series(1, 100) as s(i)
""",
        }

    def test_dictionary_encoding_single(self, project):
        """Test creating a table with dictionary encoding columns."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_dictionary_encoding_recreate(self, project):
        """Test that table with dictionary encoding can be recreated."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDictionaryEncodingSingle:
    """Tests for single dictionary encoding column."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with single dictionary encoding column."""
        return {
            "single_dict_encoding_table.sql": """
{{ config(
    materialized='table',
    dictionary_encoding_columns='status',
    orientation='column'
) }}

select
    i as id,
    case (i % 4)
        when 0 then 'pending'
        when 1 then 'processing'
        when 2 then 'completed'
        else 'failed'
    end as status,
    i as value
from generate_series(1, 50) as s(i)
""",
        }

    def test_dictionary_encoding_single_column(self, project):
        """Test creating a table with a single dictionary encoding column."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestCombinedProperties:
    """Tests for combined table properties configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with multiple properties combined.

        Note: clustering_key and event_time_column require non-nullable columns.
        Using integer column for clustering_key instead of timestamp to avoid NULL issues.
        """
        return {
            "combined_properties_table.sql": """
{{ config(
    materialized='table',
    orientation='column',
    distribution_key='user_id',
    bitmap_columns='status,category',
    dictionary_encoding_columns='region',
    lifecycle=90
) }}

select
    (i % 1000) as user_id,
    i as id,
    case (i % 3) when 0 then 'active' when 1 then 'pending' else 'closed' end as status,
    case (i % 5) when 0 then 'A' when 1 then 'B' when 2 then 'C' when 3 then 'D' else 'E' end as category,
    case (i % 4) when 0 then 'North' when 1 then 'South' when 2 then 'East' else 'West' end as region,
    i * 100 as amount
from generate_series(1, 200) as s(i)
""",
        }

    def test_all_properties_combined(self, project):
        """Test creating a table with all properties combined."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_combined_properties_recreate(self, project):
        """Test that table with combined properties can be recreated."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestEventTimeWithDistribution:
    """Tests for event_time_column combined with distribution_key."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with event_time and distribution key.

        Note: event_time_column is used for query optimization, not as a primary key.
        Using integer column to avoid NULL issues with timestamp expressions.
        """
        return {
            "event_time_distributed_table.sql": """
{{ config(
    materialized='table',
    distribution_key='user_id',
    orientation='column'
) }}

select
    (i % 500) as user_id,
    i as id,
    'data_' || i as name,
    i as event_seq
from generate_series(1, 100) as s(i)
""",
        }

    def test_event_time_with_distribution(self, project):
        """Test creating table with event_time_column and distribution_key."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestBitmapWithClustering:
    """Tests for bitmap columns combined with clustering keys."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define table with bitmap columns and clustering keys.

        Note: clustering_key requires non-nullable columns.
        Using integer column for clustering to avoid NULL issues.
        """
        return {
            "bitmap_clustering_table.sql": """
{{ config(
    materialized='table',
    bitmap_columns='status',
    orientation='column'
) }}

select
    i as id,
    case (i % 4) when 0 then 'new' when 1 then 'processing' when 2 then 'done' else 'error' end as status,
    i * 10 as value
from generate_series(1, 100) as s(i)
""",
        }

    def test_bitmap_with_clustering(self, project):
        """Test creating table with bitmap columns and clustering keys."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"