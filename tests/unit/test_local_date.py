"""
Unit tests for LocalDate class and parse_date function.

These tests verify the date manipulation functionality used in dbt Jinja2 templates.
"""

import pytest
from datetime import date, datetime

from dbt.adapters.hologres.local_date import LocalDate, parse_date, today


class TestLocalDateCreation:
    """Test LocalDate initialization from various input types."""
    
    def test_from_date_string_iso(self):
        """Test parsing ISO format date string."""
        ld = LocalDate("2024-01-15")
        assert ld.year == 2024
        assert ld.month == 1
        assert ld.day == 15
    
    def test_from_date_string_slash(self):
        """Test parsing slash format date string."""
        ld = LocalDate("2024/03/20")
        assert ld.year == 2024
        assert ld.month == 3
        assert ld.day == 20
    
    def test_from_date_string_compact(self):
        """Test parsing compact format date string."""
        ld = LocalDate("20240515")
        assert ld.year == 2024
        assert ld.month == 5
        assert ld.day == 15
    
    def test_from_datetime_string(self):
        """Test parsing datetime string."""
        ld = LocalDate("2024-01-15T10:30:00")
        assert ld.year == 2024
        assert ld.month == 1
        assert ld.day == 15
    
    def test_from_date_object(self):
        """Test creating from date object."""
        d = date(2024, 6, 10)
        ld = LocalDate(d)
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 10
    
    def test_from_datetime_object(self):
        """Test creating from datetime object."""
        dt = datetime(2024, 7, 20, 15, 30)
        ld = LocalDate(dt)
        assert ld.year == 2024
        assert ld.month == 7
        assert ld.day == 20
    
    def test_from_none_returns_today(self):
        """Test that None returns today's date."""
        ld = LocalDate(None)
        today_date = date.today()
        assert ld.year == today_date.year
        assert ld.month == today_date.month
        assert ld.day == today_date.day
    
    def test_invalid_date_string_raises_error(self):
        """Test that invalid date string raises ValueError."""
        with pytest.raises(ValueError):
            LocalDate("not-a-date")


class TestLocalDateSubtraction:
    """Test LocalDate subtraction methods."""
    
    def test_sub_days(self):
        """Test subtracting days."""
        ld = LocalDate("2024-01-15")
        result = ld.sub_days(10)
        assert str(result) == "2024-01-05"
    
    def test_sub_days_cross_month(self):
        """Test subtracting days across month boundary."""
        ld = LocalDate("2024-02-05")
        result = ld.sub_days(10)
        assert str(result) == "2024-01-26"
    
    def test_sub_days_cross_year(self):
        """Test subtracting days across year boundary."""
        ld = LocalDate("2024-01-05")
        result = ld.sub_days(10)
        assert str(result) == "2023-12-26"
    
    def test_sub_months(self):
        """Test subtracting months."""
        ld = LocalDate("2024-03-15")
        result = ld.sub_months(2)
        assert str(result) == "2024-01-15"
    
    def test_sub_months_cross_year(self):
        """Test subtracting months across year boundary."""
        ld = LocalDate("2024-02-15")
        result = ld.sub_months(3)
        assert str(result) == "2023-11-15"
    
    def test_sub_months_day_overflow(self):
        """Test subtracting months with day overflow (31st to 30-day month)."""
        ld = LocalDate("2024-03-31")
        result = ld.sub_months(1)
        # February doesn't have 31 days, should cap to 29 (leap year)
        assert str(result) == "2024-02-29"
    
    def test_sub_months_leap_year(self):
        """Test subtracting months handling leap year."""
        ld = LocalDate("2024-03-29")
        result = ld.sub_months(1)
        assert str(result) == "2024-02-29"
    
    def test_sub_years(self):
        """Test subtracting years."""
        ld = LocalDate("2024-06-15")
        result = ld.sub_years(2)
        assert str(result) == "2022-06-15"
    
    def test_sub_years_leap_year_to_non_leap(self):
        """Test subtracting years from Feb 29 to non-leap year."""
        ld = LocalDate("2024-02-29")
        result = ld.sub_years(1)
        # 2023 is not a leap year, Feb 29 doesn't exist
        assert str(result) == "2023-02-28"


class TestLocalDateAddition:
    """Test LocalDate addition methods."""
    
    def test_add_days(self):
        """Test adding days."""
        ld = LocalDate("2024-01-15")
        result = ld.add_days(10)
        assert str(result) == "2024-01-25"
    
    def test_add_days_cross_month(self):
        """Test adding days across month boundary."""
        ld = LocalDate("2024-01-25")
        result = ld.add_days(10)
        assert str(result) == "2024-02-04"
    
    def test_add_months(self):
        """Test adding months."""
        ld = LocalDate("2024-01-15")
        result = ld.add_months(3)
        assert str(result) == "2024-04-15"
    
    def test_add_months_cross_year(self):
        """Test adding months across year boundary."""
        ld = LocalDate("2024-11-15")
        result = ld.add_months(3)
        assert str(result) == "2025-02-15"
    
    def test_add_years(self):
        """Test adding years."""
        ld = LocalDate("2024-06-15")
        result = ld.add_years(2)
        assert str(result) == "2026-06-15"


class TestLocalDatePeriodBoundaries:
    """Test LocalDate period boundary methods."""
    
    def test_start_of_month(self):
        """Test getting first day of month."""
        ld = LocalDate("2024-03-15")
        result = ld.start_of_month()
        assert str(result) == "2024-03-01"
    
    def test_end_of_month(self):
        """Test getting last day of month."""
        ld = LocalDate("2024-03-15")
        result = ld.end_of_month()
        assert str(result) == "2024-03-31"
    
    def test_end_of_month_february_leap_year(self):
        """Test end of February in leap year."""
        ld = LocalDate("2024-02-15")
        result = ld.end_of_month()
        assert str(result) == "2024-02-29"
    
    def test_end_of_month_february_non_leap_year(self):
        """Test end of February in non-leap year."""
        ld = LocalDate("2023-02-15")
        result = ld.end_of_month()
        assert str(result) == "2023-02-28"
    
    def test_start_of_quarter_q1(self):
        """Test getting first day of Q1."""
        ld = LocalDate("2024-02-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-01-01"
    
    def test_start_of_quarter_q2(self):
        """Test getting first day of Q2."""
        ld = LocalDate("2024-05-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-04-01"
    
    def test_start_of_quarter_q3(self):
        """Test getting first day of Q3."""
        ld = LocalDate("2024-08-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-07-01"
    
    def test_start_of_quarter_q4(self):
        """Test getting first day of Q4."""
        ld = LocalDate("2024-11-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-10-01"
    
    def test_end_of_quarter_q1(self):
        """Test getting last day of Q1."""
        ld = LocalDate("2024-02-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-03-31"
    
    def test_end_of_quarter_q2(self):
        """Test getting last day of Q2."""
        ld = LocalDate("2024-05-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-06-30"
    
    def test_start_of_year(self):
        """Test getting first day of year."""
        ld = LocalDate("2024-06-15")
        result = ld.start_of_year()
        assert str(result) == "2024-01-01"
    
    def test_end_of_year(self):
        """Test getting last day of year."""
        ld = LocalDate("2024-06-15")
        result = ld.end_of_year()
        assert str(result) == "2024-12-31"


class TestLocalDateChaining:
    """Test chaining multiple LocalDate operations."""
    
    def test_chain_sub_months_start_of_month(self):
        """Test chaining sub_months and start_of_month."""
        ld = LocalDate("2024-03-15")
        result = ld.sub_months(2).start_of_month()
        assert str(result) == "2024-01-01"
    
    def test_chain_sub_months_start_of_quarter(self):
        """Test chaining sub_months and start_of_quarter - user's example."""
        ld = LocalDate("2024-03-15")
        result = ld.sub_months(2).start_of_quarter()
        # 2024-03-15 - 2 months = 2024-01-15, start of Q1 = 2024-01-01
        assert str(result) == "2024-01-01"
    
    def test_user_example_full(self):
        """Test the full user example with all date calculations."""
        ds = LocalDate("2024-03-15")
        
        start_quarter = ds.sub_months(2).start_of_quarter()
        start_date = ds.sub_months(2).start_of_month()
        end_date = ds.start_of_month()
        
        start_date_last = start_date.sub_months(12)
        end_date_last = end_date.sub_months(12)
        
        assert str(start_quarter) == "2024-01-01"
        assert str(start_date) == "2024-01-01"
        assert str(end_date) == "2024-03-01"
        assert str(start_date_last) == "2023-01-01"
        assert str(end_date_last) == "2023-03-01"


class TestLocalDateProperties:
    """Test LocalDate property accessors."""
    
    def test_year_property(self):
        """Test year property."""
        ld = LocalDate("2024-06-15")
        assert ld.year == 2024
    
    def test_month_property(self):
        """Test month property."""
        ld = LocalDate("2024-06-15")
        assert ld.month == 6
    
    def test_day_property(self):
        """Test day property."""
        ld = LocalDate("2024-06-15")
        assert ld.day == 15
    
    def test_quarter_property(self):
        """Test quarter property."""
        assert LocalDate("2024-01-15").quarter == 1
        assert LocalDate("2024-04-15").quarter == 2
        assert LocalDate("2024-07-15").quarter == 3
        assert LocalDate("2024-10-15").quarter == 4


class TestLocalDateFormatting:
    """Test LocalDate formatting methods."""
    
    def test_str_default_format(self):
        """Test default string format (YYYY-MM-DD)."""
        ld = LocalDate("2024-06-15")
        assert str(ld) == "2024-06-15"
    
    def test_format_custom(self):
        """Test custom format."""
        ld = LocalDate("2024-06-15")
        assert ld.format("%Y%m%d") == "20240615"
        assert ld.format("%Y/%m/%d") == "2024/06/15"
        assert ld.format("%d-%m-%Y") == "15-06-2024"
    
    def test_repr(self):
        """Test repr format."""
        ld = LocalDate("2024-06-15")
        assert repr(ld) == "LocalDate('2024-06-15')"


class TestLocalDateComparison:
    """Test LocalDate comparison methods."""
    
    def test_is_before(self):
        """Test is_before comparison."""
        ld1 = LocalDate("2024-01-15")
        ld2 = LocalDate("2024-02-15")
        assert ld1.is_before(ld2) is True
        assert ld2.is_before(ld1) is False
    
    def test_is_after(self):
        """Test is_after comparison."""
        ld1 = LocalDate("2024-02-15")
        ld2 = LocalDate("2024-01-15")
        assert ld1.is_after(ld2) is True
        assert ld2.is_after(ld1) is False
    
    def test_is_equal(self):
        """Test is_equal comparison."""
        ld1 = LocalDate("2024-01-15")
        ld2 = LocalDate("2024-01-15")
        ld3 = LocalDate("2024-01-16")
        assert ld1.is_equal(ld2) is True
        assert ld1.is_equal(ld3) is False
    
    def test_days_between(self):
        """Test days_between calculation."""
        ld1 = LocalDate("2024-01-01")
        ld2 = LocalDate("2024-01-10")
        assert ld1.days_between(ld2) == 9
        assert ld2.days_between(ld1) == -9
    
    def test_equality_operator(self):
        """Test == operator."""
        ld1 = LocalDate("2024-01-15")
        ld2 = LocalDate("2024-01-15")
        assert ld1 == ld2
    
    def test_less_than_operator(self):
        """Test < operator."""
        ld1 = LocalDate("2024-01-15")
        ld2 = LocalDate("2024-02-15")
        assert ld1 < ld2


class TestParseDateFunction:
    """Test the parse_date function."""
    
    def test_parse_date_string(self):
        """Test parse_date with string."""
        ld = parse_date("2024-06-15")
        assert str(ld) == "2024-06-15"
    
    def test_parse_date_none(self):
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
        assert str(ld) == "2024-08-20"


class TestTodayFunction:
    """Test the today function."""
    
    def test_today_returns_current_date(self):
        """Test today() returns current date."""
        ld = today()
        today_date = date.today()
        assert ld.year == today_date.year
        assert ld.month == today_date.month
        assert ld.day == today_date.day
