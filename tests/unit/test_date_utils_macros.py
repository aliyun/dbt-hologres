"""Unit tests for Hologres date utility macros.

These tests verify the date manipulation macros that use the LocalDate class
for chainable date operations in dbt Jinja2 templates.
"""
import pytest
from unittest import mock
from datetime import date
from jinja2 import Template

from dbt.adapters.hologres.local_date import LocalDate, parse_date, today


class TestParseDateMacro:
    """Test the parse_date macro functionality."""

    def test_parse_date_string_iso(self):
        """Test parse_date with ISO format string."""
        ld = parse_date("2024-01-15")
        assert ld.year == 2024
        assert ld.month == 1
        assert ld.day == 15

    def test_parse_date_string_slash(self):
        """Test parse_date with slash format string."""
        ld = parse_date("2024/03/20")
        assert ld.year == 2024
        assert ld.month == 3
        assert ld.day == 20

    def test_parse_date_string_compact(self):
        """Test parse_date with compact format string."""
        ld = parse_date("20240515")
        assert ld.year == 2024
        assert ld.month == 5
        assert ld.day == 15

    def test_parse_date_none_returns_today(self):
        """Test parse_date with None returns today."""
        ld = parse_date(None)
        today_date = date.today()
        assert ld.year == today_date.year
        assert ld.month == today_date.month
        assert ld.day == today_date.day

    def test_parse_date_date_object(self):
        """Test parse_date with date object."""
        d = date(2024, 8, 20)
        ld = parse_date(d)
        assert ld.year == 2024
        assert ld.month == 8
        assert ld.day == 20

    def test_parse_date_datetime_object(self):
        """Test parse_date with datetime object."""
        from datetime import datetime
        dt = datetime(2024, 7, 20, 15, 30)
        ld = parse_date(dt)
        assert ld.year == 2024
        assert ld.month == 7
        assert ld.day == 20

    def test_parse_date_macro_template(self):
        """Test parse_date macro in Jinja2 template."""
        template_str = """
{%- set ds = adapter.parse_date('2024-01-15') -%}
{{ ds.year }}-{{ ds.month }}-{{ ds.day }}
"""
        mock_adapter = mock.MagicMock()
        mock_adapter.parse_date = parse_date

        result = Template(template_str).render(adapter=mock_adapter).strip()
        assert result == "2024-1-15"

    def test_parse_date_invalid_string_raises_error(self):
        """Test parse_date with invalid string raises ValueError."""
        with pytest.raises(ValueError):
            parse_date("not-a-date")


class TestTodayMacro:
    """Test the today macro functionality."""

    def test_today_returns_current_date(self):
        """Test today() returns current date."""
        ld = today()
        today_date = date.today()
        assert ld.year == today_date.year
        assert ld.month == today_date.month
        assert ld.day == today_date.day

    def test_today_macro_template(self):
        """Test today macro in Jinja2 template."""
        template_str = """
{%- set current = adapter.today() -%}
{{ current.year }}-{{ current.month }}-{{ current.day }}
"""
        mock_adapter = mock.MagicMock()
        mock_adapter.today = today

        result = Template(template_str).render(adapter=mock_adapter).strip()
        today_date = date.today()
        expected = f"{today_date.year}-{today_date.month}-{today_date.day}"
        assert result == expected

    def test_today_chain_operations(self):
        """Test chaining operations on today()."""
        ld = today()
        last_week = ld.sub_days(7)
        # Verify the date is 7 days ago
        # days_between(other) returns (other._date - self._date).days
        # last_week is 7 days before ld, so last_week.days_between(ld) = 7
        assert last_week.days_between(ld) == 7


class TestDsMacro:
    """Test the ds() execution date macro functionality."""

    def test_ds_macro_default_yesterday(self):
        """Test ds() defaults to yesterday when no EXECUTION_DATE is set."""
        template_str = """
{%- set execution_date_str = var('EXECUTION_DATE', env_var('EXECUTION_DATE', 'none')) -%}
{%- if execution_date_str == 'none' -%}
    {%- set ds_val = adapter.today().sub_days(1) -%}
{%- else -%}
    {%- set ds_val = adapter.parse_date(execution_date_str) -%}
{%- endif -%}
{{ ds_val }}
"""
        mock_adapter = mock.MagicMock()
        mock_adapter.today = today
        mock_adapter.parse_date = parse_date

        result = Template(template_str).render(
            adapter=mock_adapter,
            var=lambda k, d=None: d,
            env_var=lambda k, d=None: d
        ).strip()

        # Should be yesterday's date
        expected = str(today().sub_days(1))
        assert result == expected

    def test_ds_macro_from_var(self):
        """Test ds() gets date from var('EXECUTION_DATE')."""
        template_str = """
{%- set execution_date_str = var('EXECUTION_DATE', 'none') -%}
{%- if execution_date_str == 'none' -%}
    {%- set ds_val = adapter.today().sub_days(1) -%}
{%- else -%}
    {%- set ds_val = adapter.parse_date(execution_date_str) -%}
{%- endif -%}
{{ ds_val }}
"""
        mock_adapter = mock.MagicMock()
        mock_adapter.parse_date = parse_date

        result = Template(template_str).render(
            adapter=mock_adapter,
            var=lambda k, d=None: "2024-06-15" if k == "EXECUTION_DATE" else d
        ).strip()

        assert result == "2024-06-15"

    def test_ds_macro_from_env_var(self):
        """Test ds() gets date from env_var('EXECUTION_DATE')."""
        template_str = """
{%- set execution_date_str = var('EXECUTION_DATE', env_var('EXECUTION_DATE', 'none')) -%}
{%- if execution_date_str == 'none' -%}
    {%- set ds_val = adapter.today().sub_days(1) -%}
{%- else -%}
    {%- set ds_val = adapter.parse_date(execution_date_str) -%}
{%- endif -%}
{{ ds_val }}
"""
        mock_adapter = mock.MagicMock()
        mock_adapter.parse_date = parse_date

        result = Template(template_str).render(
            adapter=mock_adapter,
            var=lambda k, d=None: d,
            env_var=lambda k, d=None: "2024-12-25" if k == "EXECUTION_DATE" else d
        ).strip()

        assert result == "2024-12-25"


class TestFormatDateMacro:
    """Test the format_date macro functionality."""

    def test_format_date_default(self):
        """Test format_date with default format."""
        ld = LocalDate("2024-06-15")
        result = ld.format()
        assert result == "2024-06-15"

    def test_format_date_custom(self):
        """Test format_date with custom format."""
        ld = LocalDate("2024-06-15")
        assert ld.format("%Y%m%d") == "20240615"
        assert ld.format("%Y/%m/%d") == "2024/06/15"
        assert ld.format("%d-%m-%Y") == "15-06-2024"

    def test_format_date_macro_template(self):
        """Test format_date macro in Jinja2 template."""
        template_str = """
{%- set ds = parse_date('2024-06-15') -%}
{{ ds.format('%Y%m%d') }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "20240615"


class TestDateRangeMacro:
    """Test the date_range macro functionality."""

    def test_date_range_single_day(self):
        """Test date_range with same start and end date."""
        template_str = """
{%- set start = parse_date('2024-01-15') -%}
{%- set end = parse_date('2024-01-15') -%}
{%- set dates = [] -%}
{%- set days = start.days_between(end) -%}
{%- for i in range(days + 1) -%}
    {%- do dates.append(start.add_days(i)) -%}
{%- endfor -%}
{%- for d in dates -%}{{ d }}{% if not loop.last %}, {% endif %}{% endfor -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            parse_date=parse_date
        ).strip()
        assert result == "2024-01-15"

    def test_date_range_multiple_days(self):
        """Test date_range with multiple days."""
        template_str = """
{%- set start = parse_date('2024-01-01') -%}
{%- set end = parse_date('2024-01-05') -%}
{%- set dates = [] -%}
{%- set days = start.days_between(end) -%}
{%- for i in range(days + 1) -%}
    {%- do dates.append(start.add_days(i)) -%}
{%- endfor -%}
{%- for d in dates -%}{{ d }}{% if not loop.last %}, {% endif %}{% endfor -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            parse_date=parse_date
        ).strip()
        assert result == "2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05"

    def test_date_range_cross_month(self):
        """Test date_range crossing month boundary."""
        template_str = """
{%- set start = parse_date('2024-01-29') -%}
{%- set end = parse_date('2024-02-02') -%}
{%- set dates = [] -%}
{%- set days = start.days_between(end) -%}
{%- for i in range(days + 1) -%}
    {%- do dates.append(start.add_days(i)) -%}
{%- endfor -%}
{%- for d in dates -%}{{ d }}{% if not loop.last %}, {% endif %}{% endfor -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            parse_date=parse_date
        ).strip()
        assert result == "2024-01-29, 2024-01-30, 2024-01-31, 2024-02-01, 2024-02-02"


class TestMonthsBetweenMacro:
    """Test the months_between macro functionality."""

    def test_months_between_same_month(self):
        """Test months_between in same month."""
        template_str = """
{%- set start = parse_date('2024-01-01') -%}
{%- set end = parse_date('2024-01-31') -%}
{%- set days = start.days_between(end) -%}
{{ (days / 30) | int }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "1"

    def test_months_between_cross_year(self):
        """Test months_between crossing year boundary."""
        template_str = """
{%- set start = parse_date('2024-01-01') -%}
{%- set end = parse_date('2025-01-01') -%}
{%- set days = start.days_between(end) -%}
{{ (days / 30) | int }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        # 366 days / 30 = 12.2 -> 12
        assert result == "12"

    def test_months_between_approximate(self):
        """Test months_between is approximate (30-day months)."""
        template_str = """
{%- set start = parse_date('2024-01-15') -%}
{%- set end = parse_date('2024-04-15') -%}
{%- set days = start.days_between(end) -%}
{{ (days / 30) | int }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        # 91 days / 30 = 3.03 -> 3
        assert result == "3"


class TestDateMacroChaining:
    """Test chaining multiple date macro operations."""

    def test_chain_sub_months_start_of_month(self):
        """Test chaining sub_months and start_of_month."""
        template_str = """
{%- set ds = parse_date('2024-03-15') -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{{ start_date }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "2024-01-01"

    def test_chain_sub_months_start_of_quarter(self):
        """Test chaining sub_months and start_of_quarter."""
        template_str = """
{%- set ds = parse_date('2024-03-15') -%}
{%- set start_quarter = ds.sub_months(2).start_of_quarter() -%}
{{ start_quarter }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "2024-01-01"

    def test_complex_date_calculation(self):
        """Test complex date calculation example."""
        template_str = """
{%- set ds = parse_date('2024-03-15') -%}
{%- set start_quarter = ds.sub_months(2).start_of_quarter() -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{%- set end_date = ds.start_of_month() -%}
{%- set start_date_last = start_date.sub_months(12) -%}
{%- set end_date_last = end_date.sub_months(12) -%}
{{ start_quarter }},{{ start_date }},{{ end_date }},{{ start_date_last }},{{ end_date_last }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "2024-01-01,2024-01-01,2024-03-01,2023-01-01,2023-03-01"


class TestDateProperties:
    """Test LocalDate property accessors in macros."""

    def test_year_property(self):
        """Test year property in template."""
        template_str = """
{%- set ds = parse_date('2024-06-15') -%}
{{ ds.year }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "2024"

    def test_month_property(self):
        """Test month property in template."""
        template_str = """
{%- set ds = parse_date('2024-06-15') -%}
{{ ds.month }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "6"

    def test_day_property(self):
        """Test day property in template."""
        template_str = """
{%- set ds = parse_date('2024-06-15') -%}
{{ ds.day }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "15"

    def test_quarter_property(self):
        """Test quarter property in template."""
        template_str = """
{%- set ds = parse_date('2024-08-15') -%}
{{ ds.quarter }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "3"


class TestDateComparison:
    """Test LocalDate comparison methods in macros."""

    def test_is_before(self):
        """Test is_before comparison in template."""
        template_str = """
{%- set ds1 = parse_date('2024-01-15') -%}
{%- set ds2 = parse_date('2024-02-15') -%}
{{ ds1.is_before(ds2) }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "True"

    def test_is_after(self):
        """Test is_after comparison in template."""
        template_str = """
{%- set ds1 = parse_date('2024-02-15') -%}
{%- set ds2 = parse_date('2024-01-15') -%}
{{ ds1.is_after(ds2) }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "True"

    def test_is_equal(self):
        """Test is_equal comparison in template."""
        template_str = """
{%- set ds1 = parse_date('2024-01-15') -%}
{%- set ds2 = parse_date('2024-01-15') -%}
{{ ds1.is_equal(ds2) }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "True"

    def test_days_between(self):
        """Test days_between calculation in template."""
        template_str = """
{%- set ds1 = parse_date('2024-01-01') -%}
{%- set ds2 = parse_date('2024-01-10') -%}
{{ ds1.days_between(ds2) }}
"""
        result = Template(template_str).render(parse_date=parse_date).strip()
        assert result == "9"