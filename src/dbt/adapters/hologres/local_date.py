"""
LocalDate - A date utility class for Jinja2 templates in dbt.

This module provides a LocalDate class similar to Java's LocalDate,
supporting chainable date operations for use in dbt models.

Example usage in Jinja2:
    {%- set ds = parse_date('2024-01-15') -%}
    {%- set start_date = ds.sub_months(2).start_of_month() -%}
"""

from datetime import date, datetime, timedelta
from typing import Union
import calendar


class LocalDate:
    """
    A date class supporting chainable operations for Jinja2 templates.
    
    Provides methods similar to Java's LocalDate API for date manipulation,
    making it easy to work with dates in dbt models.
    """
    
    def __init__(self, dt: Union[date, datetime, str, None] = None):
        """
        Initialize LocalDate from various input types.
        
        Args:
            dt: Can be a date, datetime, string (YYYY-MM-DD format), or None (uses today)
        """
        if dt is None:
            self._date = date.today()
        elif isinstance(dt, datetime):
            self._date = dt.date()
        elif isinstance(dt, date):
            self._date = dt
        elif isinstance(dt, str):
            self._date = self._parse_date_string(dt)
        else:
            raise ValueError(f"Cannot parse date from type: {type(dt)}")
    
    @staticmethod
    def _parse_date_string(date_str: str) -> date:
        """Parse date string in various formats."""
        date_str = date_str.strip()
        
        # Try common date formats
        formats = [
            "%Y-%m-%d",           # 2024-01-15
            "%Y/%m/%d",           # 2024/01/15
            "%Y%m%d",             # 20240115
            "%Y-%m-%dT%H:%M:%S",  # ISO format with time
            "%Y-%m-%d %H:%M:%S",  # datetime format
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        raise ValueError(f"Cannot parse date string: {date_str}")
    
    # ========== Subtraction Methods ==========
    
    def sub_days(self, days: int) -> "LocalDate":
        """
        Subtract days from this date.
        
        Args:
            days: Number of days to subtract
            
        Returns:
            New LocalDate instance
        """
        new_date = self._date - timedelta(days=days)
        return LocalDate(new_date)
    
    def sub_months(self, months: int) -> "LocalDate":
        """
        Subtract months from this date.
        
        Args:
            months: Number of months to subtract
            
        Returns:
            New LocalDate instance
        """
        year = self._date.year
        month = self._date.month - months
        day = self._date.day
        
        # Handle year rollover
        while month <= 0:
            month += 12
            year -= 1
        
        # Handle day overflow (e.g., Jan 31 - 1 month = Dec 31, not Dec 28)
        max_day = calendar.monthrange(year, month)[1]
        day = min(day, max_day)
        
        return LocalDate(date(year, month, day))
    
    def sub_years(self, years: int) -> "LocalDate":
        """
        Subtract years from this date.
        
        Args:
            years: Number of years to subtract
            
        Returns:
            New LocalDate instance
        """
        year = self._date.year - years
        month = self._date.month
        day = self._date.day
        
        # Handle leap year (Feb 29 -> Feb 28)
        max_day = calendar.monthrange(year, month)[1]
        day = min(day, max_day)
        
        return LocalDate(date(year, month, day))
    
    # ========== Addition Methods ==========
    
    def add_days(self, days: int) -> "LocalDate":
        """
        Add days to this date.
        
        Args:
            days: Number of days to add
            
        Returns:
            New LocalDate instance
        """
        new_date = self._date + timedelta(days=days)
        return LocalDate(new_date)
    
    def add_months(self, months: int) -> "LocalDate":
        """
        Add months to this date.
        
        Args:
            months: Number of months to add
            
        Returns:
            New LocalDate instance
        """
        year = self._date.year
        month = self._date.month + months
        day = self._date.day
        
        # Handle year rollover
        while month > 12:
            month -= 12
            year += 1
        
        # Handle day overflow
        max_day = calendar.monthrange(year, month)[1]
        day = min(day, max_day)
        
        return LocalDate(date(year, month, day))
    
    def add_years(self, years: int) -> "LocalDate":
        """
        Add years to this date.
        
        Args:
            years: Number of years to add
            
        Returns:
            New LocalDate instance
        """
        year = self._date.year + years
        month = self._date.month
        day = self._date.day
        
        # Handle leap year
        max_day = calendar.monthrange(year, month)[1]
        day = min(day, max_day)
        
        return LocalDate(date(year, month, day))
    
    # ========== Start/End of Period Methods ==========
    
    def start_of_month(self) -> "LocalDate":
        """
        Get the first day of this date's month.
        
        Returns:
            New LocalDate instance representing the first day of the month
        """
        return LocalDate(date(self._date.year, self._date.month, 1))
    
    def end_of_month(self) -> "LocalDate":
        """
        Get the last day of this date's month.
        
        Returns:
            New LocalDate instance representing the last day of the month
        """
        max_day = calendar.monthrange(self._date.year, self._date.month)[1]
        return LocalDate(date(self._date.year, self._date.month, max_day))
    
    def start_of_quarter(self) -> "LocalDate":
        """
        Get the first day of this date's quarter.
        
        Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
        
        Returns:
            New LocalDate instance representing the first day of the quarter
        """
        quarter = (self._date.month - 1) // 3
        first_month_of_quarter = quarter * 3 + 1
        return LocalDate(date(self._date.year, first_month_of_quarter, 1))
    
    def end_of_quarter(self) -> "LocalDate":
        """
        Get the last day of this date's quarter.
        
        Returns:
            New LocalDate instance representing the last day of the quarter
        """
        quarter = (self._date.month - 1) // 3
        last_month_of_quarter = quarter * 3 + 3
        max_day = calendar.monthrange(self._date.year, last_month_of_quarter)[1]
        return LocalDate(date(self._date.year, last_month_of_quarter, max_day))
    
    def start_of_year(self) -> "LocalDate":
        """
        Get the first day of this date's year.
        
        Returns:
            New LocalDate instance representing January 1st
        """
        return LocalDate(date(self._date.year, 1, 1))
    
    def end_of_year(self) -> "LocalDate":
        """
        Get the last day of this date's year.
        
        Returns:
            New LocalDate instance representing December 31st
        """
        return LocalDate(date(self._date.year, 12, 31))
    
    def start_of_week(self, start_day: int = 0) -> "LocalDate":
        """
        Get the first day of this date's week.
        
        Args:
            start_day: Week start day (0=Monday, 6=Sunday). Default is Monday.
            
        Returns:
            New LocalDate instance representing the first day of the week
        """
        current_weekday = self._date.weekday()
        days_to_subtract = (current_weekday - start_day) % 7
        return self.sub_days(days_to_subtract)
    
    def end_of_week(self, start_day: int = 0) -> "LocalDate":
        """
        Get the last day of this date's week.
        
        Args:
            start_day: Week start day (0=Monday, 6=Sunday). Default is Monday.
            
        Returns:
            New LocalDate instance representing the last day of the week
        """
        return self.start_of_week(start_day).add_days(6)
    
    # ========== Property Accessors ==========
    
    @property
    def year(self) -> int:
        """Get the year component."""
        return self._date.year
    
    @property
    def month(self) -> int:
        """Get the month component (1-12)."""
        return self._date.month
    
    @property
    def day(self) -> int:
        """Get the day component (1-31)."""
        return self._date.day
    
    @property
    def quarter(self) -> int:
        """Get the quarter (1-4)."""
        return (self._date.month - 1) // 3 + 1
    
    @property
    def day_of_week(self) -> int:
        """Get the day of week (0=Monday, 6=Sunday)."""
        return self._date.weekday()
    
    @property
    def day_of_year(self) -> int:
        """Get the day of year (1-366)."""
        return self._date.timetuple().tm_yday
    
    # ========== Formatting Methods ==========
    
    def format(self, fmt: str = "%Y-%m-%d") -> str:
        """
        Format the date as a string.
        
        Args:
            fmt: Date format string (default: YYYY-MM-DD)
            
        Returns:
            Formatted date string
        """
        return self._date.strftime(fmt)
    
    def to_date(self) -> date:
        """
        Get the underlying Python date object.
        
        Returns:
            Python date object
        """
        return self._date
    
    # ========== Comparison Methods ==========
    
    def is_before(self, other: "LocalDate") -> bool:
        """Check if this date is before another date."""
        return self._date < other._date
    
    def is_after(self, other: "LocalDate") -> bool:
        """Check if this date is after another date."""
        return self._date > other._date
    
    def is_equal(self, other: "LocalDate") -> bool:
        """Check if this date equals another date."""
        return self._date == other._date
    
    def days_between(self, other: "LocalDate") -> int:
        """
        Calculate the number of days between this date and another.
        
        Args:
            other: Another LocalDate
            
        Returns:
            Number of days (positive if other is after this date)
        """
        return (other._date - self._date).days
    
    # ========== Magic Methods ==========
    
    def __str__(self) -> str:
        """String representation in YYYY-MM-DD format."""
        return self.format()
    
    def __repr__(self) -> str:
        """Debug representation."""
        return f"LocalDate('{self.format()}')"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, LocalDate):
            return self._date == other._date
        return False
    
    def __lt__(self, other: "LocalDate") -> bool:
        return self._date < other._date
    
    def __le__(self, other: "LocalDate") -> bool:
        return self._date <= other._date
    
    def __gt__(self, other: "LocalDate") -> bool:
        return self._date > other._date
    
    def __ge__(self, other: "LocalDate") -> bool:
        return self._date >= other._date
    
    def __hash__(self) -> int:
        return hash(self._date)


def parse_date(date_input: Union[str, date, datetime, None] = None) -> LocalDate:
    """
    Parse a date string or object into a LocalDate instance.
    
    This function is designed to be used in Jinja2 templates within dbt models.
    
    Args:
        date_input: Date string (YYYY-MM-DD), date object, datetime object, or None (today)
        
    Returns:
        LocalDate instance supporting chainable date operations
        
    Example:
        {%- set ds = parse_date('2024-01-15') -%}
        {%- set start_date = ds.sub_months(2).start_of_month() -%}
    """
    return LocalDate(date_input)


def today() -> LocalDate:
    """
    Get today's date as a LocalDate.
    
    Returns:
        LocalDate instance for today
    """
    return LocalDate()
