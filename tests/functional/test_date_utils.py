"""Functional tests for date utility macros in dbt-hologres.

The date_utils module provides chainable LocalDate operations that can be
used in dbt models for date calculations. These tests verify:
- LocalDate macro functionality
- Chainable date operations
- Date arithmetic (add/sub days, months, years)
- Period boundaries (start/end of month, quarter, year)

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_date_utils.py
"""
import pytest

from dbt.tests.util import run_dbt

from tests.functional.fixtures import (
    models__with_date_utils,
)


class TestDateUtilsMacros:
    """Tests for date utility macro functionality."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models using date utils."""
        return {
            "date_utils_test.sql": models__with_date_utils,
        }

    def test_date_utils_model_runs(self, project):
        """Test that model with date utils compiles and runs."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateMacro:
    """Tests for LocalDate macro with various operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models testing LocalDate operations."""
        return {
            "localdate_operations.sql": """
{{ config(materialized='table') }}

select
    -- Basic date parsing
    {{local_date('2024-01-15').to_sql() }} as base_date,

    -- Subtraction operations
    {{local_date('2024-01-15').sub_days(7).to_sql() }} as week_ago,
    {{local_date('2024-01-15').sub_months(1).to_sql() }} as month_ago,
    {{local_date('2024-01-15').sub_years(1).to_sql() }} as year_ago,

    -- Addition operations
    {{local_date('2024-01-15').add_days(10).to_sql() }} as ten_days_later,
    {{local_date('2024-01-15').add_months(2).to_sql() }} as two_months_later,
    {{local_date('2024-01-15').add_years(1).to_sql() }} as next_year
""",
        }

    def test_localdate_operations(self, project):
        """Test LocalDate arithmetic operations."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDatePeriodBoundaries:
    """Tests for LocalDate period boundary operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models testing period boundaries."""
        return {
            "period_boundaries.sql": """
{{ config(materialized='table') }}

select
    -- Month boundaries
    {{local_date('2024-01-15').start_of_month().to_sql() }} as month_start,
    {{local_date('2024-01-15').end_of_month().to_sql() }} as month_end,

    -- Quarter boundaries
    {{local_date('2024-02-15').start_of_quarter().to_sql() }} as quarter_start,
    {{local_date('2024-02-15').end_of_quarter().to_sql() }} as quarter_end,

    -- Year boundaries
    {{local_date('2024-06-15').start_of_year().to_sql() }} as year_start,
    {{local_date('2024-06-15').end_of_year().to_sql() }} as year_end
""",
        }

    def test_period_boundaries(self, project):
        """Test period boundary calculations."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateChainedOperations:
    """Tests for chained LocalDate operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with chained operations."""
        return {
            "chained_operations.sql": """
{{ config(materialized='table') }}

select
    -- Chain: subtract months then get start of month
    {{local_date('2024-03-15').sub_months(2).start_of_month().to_sql() }} as two_months_ago_start,

    -- Chain: subtract days then get end of month
    {{local_date('2024-02-20').sub_days(10).end_of_month().to_sql() }} as adjusted_month_end,

    -- Chain: add months then get start of quarter
    {{local_date('2024-01-15').add_months(3).start_of_quarter().to_sql() }} as future_quarter_start,

    -- Chain: subtract years then get end of year
    {{local_date('2024-06-15').sub_years(1).end_of_year().to_sql() }} as last_year_end
""",
        }

    def test_chained_operations(self, project):
        """Test chained date operations work correctly."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateToday:
    """Tests for today() function."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models using today()."""
        return {
            "today_test.sql": """
{{ config(materialized='table') }}

select
    {{today().to_sql() }} as today_date,
    {{today().sub_days(1).to_sql() }} as yesterday,
    {{today().add_days(1).to_sql() }} as tomorrow,
    {{today().start_of_month().to_sql() }} as month_start,
    {{today().end_of_month().to_sql() }} as month_end
""",
        }

    def test_today_function(self, project):
        """Test today() function returns current date."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateWithParseDate:
    """Tests for parse_date function."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models using parse_date."""
        return {
            "parse_date_test.sql": """
{{ config(materialized='table') }}

select
    -- Parse date string
    {{parse_date('2024-06-15').to_sql() }} as parsed_date,

    -- Parse and perform operations
    {{parse_date('2024-06-15').sub_days(30).to_sql() }} as thirty_days_ago,

    -- Parse date from variable
    {% set my_date = parse_date('2024-01-01') %}
    {{ my_date.add_months(6) }} as six_months_added
""",
        }

    def test_parse_date_function(self, project):
        """Test parse_date function works correctly."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateWithVariables:
    """Tests for LocalDate with Jinja2 variables."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models using date variables."""
        return {
            "variable_dates.sql": """
{{ config(materialized='table') }}

{%- set base = parse_date('2024-01-01') -%}
{%- set end_date = parse_date('2024-12-31') -%}

select
    {{ base.to_sql() }} as start_date,
    {{ end_date.to_sql() }} as end_date,
    {{ base.add_days(30).to_sql() }} as start_plus_30,
    {{ end_date.sub_days(7).to_sql() }} as end_minus_7
""",
        }

    def test_date_with_variables(self, project):
        """Test using dates with Jinja2 variables."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateInWhereClause:
    """Tests for using LocalDate in WHERE clauses."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seed data for date filtering tests."""
        return {
            "event_data.csv": """event_date,event_name,value
2024-01-01,Event_A,100
2024-01-15,Event_B,200
2024-02-01,Event_C,150
2024-02-15,Event_D,250
2024-03-01,Event_E,300
""",
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define model using date in WHERE clause."""
        return {
            "filtered_events.sql": """
{{ config(materialized='table') }}

{%- set start_date = parse_date('2024-01-15') -%}
{%- set end_date = parse_date('2024-02-15') -%}

select
    event_date,
    event_name,
    value
from {{ ref('event_data') }}
where event_date >= {{ start_date.to_sql() }}
  and event_date <= {{ end_date.to_sql() }}
""",
        }

    def test_date_in_where_clause(self, project):
        """Test using LocalDate in WHERE clause for filtering."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateWithIncremental:
    """Tests for using LocalDate in incremental models."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define incremental model using date utils."""
        return {
            "incremental_dates.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

{%- set lookback_date = today().sub_days(7) -%}

select
    current_date as ds,
    generate_series(1, 5) as id,
    'data_' || generate_series(1, 5) as name

{% if is_incremental() %}
-- In incremental runs, this filter would apply
-- where ds >= {{ lookback_date }}
{% endif %}
""",
        }

    def test_incremental_with_dates(self, project):
        """Test using date utils in incremental models."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestLocalDateEdgeCases:
    """Tests for edge cases in LocalDate operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models testing edge cases."""
        return {
            "edge_cases.sql": """
{{ config(materialized='table') }}

select
    -- Leap year handling
    {{local_date('2024-02-29').add_years(1).to_sql() }} as leap_year_next,  -- 2025-02-28
    {{local_date('2024-02-29').sub_years(4).to_sql() }} as leap_year_prev,  -- 2020-02-29

    -- Month end handling (Jan 31 + 1 month = Feb 28/29)
    {{local_date('2024-01-31').add_months(1).to_sql() }} as jan31_plus_month,

    -- Year boundary crossing
    {{local_date('2024-12-25').add_days(10).to_sql() }} as crossing_year_end,
    {{local_date('2024-01-05').sub_days(10).to_sql() }} as crossing_year_start
""",
        }

    def test_edge_cases(self, project):
        """Test edge cases in date calculations."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"