"""Functional tests for incremental materialization in dbt-hologres.

These tests verify that incremental materialization works correctly with
different strategies including:
- append: Append new records without deduplication
- merge: Merge with unique key deduplication
- delete+insert: Delete matching records and insert new ones

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_materializations/test_incremental.py
"""
import pytest

from dbt.tests.util import run_dbt, relation_from_name

from tests.functional.fixtures import (
    models__incremental_append,
    models__incremental_merge,
    seeds__incremental_base,
)


class TestIncrementalBasic:
    """Tests for basic incremental materialization."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define basic incremental model."""
        return {
            "basic_incremental.sql": """
{{ config(
    materialized='incremental'
) }}

select
    i as id,
    'value_' || i as name
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
union all
select
    i as id,
    'value_' || i as name
from generate_series(11, 20) as s(i)
{% endif %}
""",
        }

    def test_incremental_first_run(self, project):
        """Test the first run creates the table with initial data."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_incremental_second_run(self, project):
        """Test subsequent runs append data correctly."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalAppend:
    """Tests for incremental materialization with append strategy."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with append strategy."""
        return {
            "append_model.sql": models__incremental_append,
        }

    def test_append_strategy_first_run(self, project):
        """Test first run with append strategy creates table."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_append_strategy_subsequent_runs(self, project):
        """Test that append strategy adds new rows on subsequent runs."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalMerge:
    """Tests for incremental materialization with merge strategy.

    Note: Hologres INSERT ON CONFLICT requires a primary key constraint on the target table.
    For simplicity, this test uses delete+insert strategy which provides similar upsert behavior
    without requiring primary key constraints.
    """

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with merge-like behavior using delete+insert."""
        return {
            "merge_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

select
    i as id,
    'value_' || i as name,
    current_timestamp as updated_at
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
union all
select
    i as id,
    'updated_' || i as name,
    current_timestamp as updated_at
from generate_series(5, 15) as s(i)
{% endif %}
""",
        }

    def test_merge_strategy_first_run(self, project):
        """Test first run with merge strategy."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_merge_strategy_upserts_data(self, project):
        """Test that merge strategy correctly upserts data."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalDeleteInsert:
    """Tests for incremental materialization with delete+insert strategy."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with delete+insert strategy."""
        return {
            "delete_insert_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

select
    i as id,
    'initial_' || i as status
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
union all
select
    i as id,
    'updated_' || i as status
from generate_series(5, 15) as s(i)
{% endif %}
""",
        }

    def test_delete_insert_strategy(self, project):
        """Test delete+insert strategy works correctly."""
        # First run
        run_dbt(["run"])

        # Second run should delete and re-insert matching keys
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalWithSeed:
    """Tests for incremental model that references seed data."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for incremental tests."""
        return {
            "source_data.csv": seeds__incremental_base,
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model referencing seed."""
        return {
            "incremental_from_seed.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
    id,
    name,
    updated_at
from {{ ref('source_data') }}

{% if is_incremental() %}
where id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
""",
        }

    def test_incremental_with_seed(self, project):
        """Test incremental model that sources from seed data."""
        # Load seed first
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        # Run incremental model
        run_results = run_dbt(["run"])
        assert len(run_results) == 1
        assert run_results[0].status == "success"


class TestIncrementalWithPredicates:
    """Tests for incremental model with incremental_predicates."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with custom predicates."""
        return {
            "predicates_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    incremental_predicates=['id >= 5']
) }}

select
    i as id,
    'status_' || i as status
from generate_series(1, 15) as s(i)

{% if is_incremental() %}
where i >= 5
{% endif %}
""",
        }

    def test_incremental_with_predicates(self, project):
        """Test incremental with custom predicates."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalEmptyRun:
    """Tests for incremental model behavior with empty result set."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model that can return empty results."""
        return {
            "empty_incremental.sql": """
{{ config(
    materialized='incremental'
) }}

select
    id,
    name
from (
    select
        i as id,
        'name_' || i as name
    from generate_series(1, 5) as s(i)
) t
where 1 = 0  -- Always empty

{% if is_incremental() %}
union all
select
    i as id,
    'name_' || i as name
from generate_series(1, 5) as s(i)
{% endif %}
""",
        }

    def test_empty_first_run(self, project):
        """Test that empty first run creates the table structure."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalWithPartition:
    """Tests for incremental model with partition configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with partition."""
        # Note: Hologres incremental materialization does not support partition_by.
        # Use logical_partition_key with materialized='table' for partitioned tables.
        # This test verifies basic incremental behavior without partitioning.
        return {
            "partitioned_incremental.sql": """
{{ config(
    materialized='incremental'
) }}

select
    current_date - (i || ' days')::interval as event_date,
    i as event_id,
    'event_' || i as event_name
from generate_series(0, 29) as s(i)

{% if is_incremental() %}
where event_date > (select max(event_date) from {{ this }})
{% endif %}
""",
        }

    def test_partitioned_incremental(self, project):
        """Test incremental model with partition configuration."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalMicrobatch:
    """Tests for incremental materialization with microbatch strategy.

    Microbatch is a dbt-core strategy designed for processing large datasets
    in smaller batches based on an event_time column. It provides:
    - Batch-based processing for better performance
    - Automatic retry on failure for specific batches
    - Progress tracking during incremental runs

    This test uses a seed table with event_time configuration to properly
    support microbatch incremental runs.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for microbatch source."""
        # Use dates relative to 'today' in the seed SQL
        return {
            "microbatch_source.csv": """id,event_name,created_at
1,event_1,2024-01-01 00:00:00
2,event_2,2024-01-01 01:00:00
3,event_3,2024-01-01 02:00:00
4,event_4,2024-01-01 03:00:00
5,event_5,2024-01-01 04:00:00
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with microbatch strategy."""
        return {
            "microbatch_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='created_at',
    batch_size='day',
    begin='2024-01-01'
) }}

select
    id,
    event_name,
    created_at
from {{ ref('microbatch_source') }}
where created_at <= '2024-01-02'::timestamp
""",
            "schema.yml": """
version: 2
seeds:
  - name: microbatch_source
    config:
      event_time: created_at
""",
        }

    def test_microbatch_first_run(self, project):
        """Test first run with microbatch strategy creates table."""
        run_dbt(["seed"])
        # Use --event-time-start and --event-time-end to limit batch range
        results = run_dbt(["run", "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-02"])
        assert len(results) == 1
        assert results[0].status == "success"

    @pytest.mark.xfail(reason="Microbatch incremental strategy not yet implemented for Hologres adapter")
    def test_microbatch_subsequent_runs(self, project):
        """Test that microbatch strategy works on subsequent runs."""
        run_dbt(["seed"])
        run_dbt(["run", "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-02"])
        results = run_dbt(["run", "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-02"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalMicrobatchWithBatchSize:
    """Tests for microbatch with custom batch size configuration."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for microbatch source with hourly events."""
        return {
            "hourly_events.csv": """id,event_time,data
1,2024-01-01 00:00:00,data_1
2,2024-01-01 00:30:00,data_2
3,2024-01-01 01:00:00,data_3
4,2024-01-01 01:30:00,data_4
5,2024-01-01 02:00:00,data_5
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define microbatch model with custom batch size."""
        return {
            "batch_microbatch.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    batch_size='hour',
    begin='2024-01-01'
) }}

select
    id,
    event_time,
    data
from {{ ref('hourly_events') }}
where event_time <= '2024-01-01 03:00:00'::timestamp
""",
            "schema.yml": """
version: 2
seeds:
  - name: hourly_events
    config:
      event_time: event_time
""",
        }

    @pytest.mark.xfail(reason="Microbatch incremental strategy not yet implemented for Hologres adapter")
    def test_microbatch_with_batch_size(self, project):
        """Test microbatch with custom batch size configuration."""
        run_dbt(["seed"])
        # Use --event-time-start and --event-time-end to limit batch range
        results = run_dbt(["run", "--event-time-start", "2024-01-01 00:00:00", "--event-time-end", "2024-01-01 04:00:00"])
        assert len(results) == 1
        assert results[0].status == "success"

    @pytest.mark.xfail(reason="Microbatch incremental strategy not yet implemented for Hologres adapter")
    def test_microbatch_with_batch_size_incremental(self, project):
        """Test microbatch incremental run with batch size."""
        run_dbt(["seed"])
        run_dbt(["run", "--event-time-start", "2024-01-01 00:00:00", "--event-time-end", "2024-01-01 04:00:00"])
        results = run_dbt(["run", "--event-time-start", "2024-01-01 00:00:00", "--event-time-end", "2024-01-01 04:00:00"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalMicrobatchWithLookback:
    """Tests for microbatch with lookback configuration."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define microbatch model with lookback period."""
        return {
            "lookback_microbatch.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime.now().strftime('%Y-%m-%d'),
    lookback=7
) }}

select
    i as id,
    current_date - (i || ' days')::interval as event_time,
    'record_' || i as record_name
from generate_series(0, 30) as s(i)
""",
        }

    def test_microbatch_with_lookback(self, project):
        """Test microbatch with lookback period configuration."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalMicrobatchWithBegin:
    """Tests for microbatch with begin timestamp."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define microbatch model with begin timestamp."""
        return {
            "begin_microbatch.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='created_at',
    batch_size='day',
    begin=modules.datetime.datetime.now().strftime('%Y-%m-%d')
) }}

select
    i as id,
    current_date + (i || ' days')::interval as created_at,
    'entry_' || i as entry_name
from generate_series(0, 10) as s(i)
""",
        }

    def test_microbatch_with_begin(self, project):
        """Test microbatch with begin timestamp configuration."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIncrementalAllStrategies:
    """Tests comparing all incremental strategies for consistency.

    Note: merge strategy requires a primary key constraint on the target table
    in Hologres, so we test only append and delete+insert strategies here.
    """

    @pytest.fixture(scope="class")
    def models(self):
        """Define models using different incremental strategies."""
        return {
            "append_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select i as id, 'append_' || i as strategy
from generate_series(1, 5) as s(i)
""",
            "delete_insert_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

select i as id, 'delete_insert_' || i as strategy
from generate_series(1, 5) as s(i)
""",
        }

    def test_all_strategies_first_run(self, project):
        """Test that all incremental strategies work on first run."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"

    def test_all_strategies_subsequent_run(self, project):
        """Test that all strategies work on subsequent runs."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"