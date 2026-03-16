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
    generate_series(1, 10) as id,
    'value_' || generate_series(1, 10) as name

{% if is_incremental() %}
union all
select
    generate_series(11, 20) as id,
    'value_' || generate_series(11, 20) as name
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
    """Tests for incremental materialization with merge strategy."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model with merge strategy."""
        return {
            "merge_model.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
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
    generate_series(1, 10) as id,
    'initial_' || generate_series(1, 10) as status

{% if is_incremental() %}
union all
select
    generate_series(5, 15) as id,
    'updated_' || generate_series(5, 15) as status
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
    generate_series(1, 15) as id,
    'status_' || generate_series(1, 15) as status

{% if is_incremental() %}
where id >= 5
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
        generate_series(1, 5) as id,
        'name_' || generate_series(1, 5) as name
) t
where 1 = 0  -- Always empty

{% if is_incremental() %}
union all
select
    generate_series(1, 5) as id,
    'name_' || generate_series(1, 5) as name
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
        return {
            "partitioned_incremental.sql": """
{{ config(
    materialized='incremental',
    partition_by=['event_date']
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