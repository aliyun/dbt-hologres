{#
    Date Utility Macros for Hologres dbt adapter
    
    These macros provide convenient date manipulation functions using the LocalDate class.
    LocalDate supports chainable operations similar to Java's LocalDate API.
    
    Available LocalDate methods:
    - sub_days(n), sub_months(n), sub_years(n)  - Subtract time periods
    - add_days(n), add_months(n), add_years(n)  - Add time periods
    - start_of_month(), end_of_month()          - Get month boundaries
    - start_of_quarter(), end_of_quarter()      - Get quarter boundaries
    - start_of_year(), end_of_year()            - Get year boundaries
    - start_of_week(), end_of_week()            - Get week boundaries
    - format(fmt)                               - Format as string
    - year, month, day, quarter                 - Property accessors
    
    Example usage:
        {%- set ds = parse_date('2024-01-15') -%}
        {%- set start_date = ds.sub_months(2).start_of_month() -%}
        -- Result: start_date = '2023-11-01'
#}

{%- macro parse_date(date_input=none) -%}
    {#
        Parse a date string or object into a LocalDate instance.

        Args:
            date_input: Date string (YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD),
                       date object, datetime object, or None (returns today)

        Returns:
            LocalDate instance supporting chainable date operations

        Example:
            {%- set ds = parse_date('2024-01-15') -%}
            {%- set last_month = ds.sub_months(1) -%}
            {{ last_month.to_sql() }}  -- Output: DATE '2023-12-15'

        Note: When used directly in SQL (e.g., {{ parse_date('2024-01-15') }}),
              use .to_sql() method to get SQL-compatible format: DATE '2024-01-15'
    #}
    {% do return(adapter.parse_date(date_input)) %}
{%- endmacro -%}


{%- macro local_date(date_input=none) -%}
    {#
        Alias for parse_date for convenience.
        Provided for compatibility with test fixtures and user convenience.

        Args:
            date_input: Date string (YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD),
                       date object, datetime object, or None (returns today)

        Returns:
            LocalDate instance supporting chainable date operations

        Example:
            {%- set ds = local_date('2024-01-15') -%}
            {%- set next_month = ds.add_months(1) -%}
    #}
    {% do return(adapter.parse_date(date_input)) %}
{%- endmacro -%}


{%- macro today() -%}
    {#
        Get today's date as a LocalDate instance.
        
        Returns:
            LocalDate instance for today
            
        Example:
            {%- set current = today() -%}
            {%- set last_week = current.sub_days(7) -%}
    #}
    {% do return(adapter.today()) %}
{%- endmacro -%}


{%- macro ds() -%}
    {#
        Get the execution date as a LocalDate instance.
        
        This macro checks for EXECUTION_DATE in:
        1. dbt variable: var('EXECUTION_DATE')
        2. Environment variable: env_var('EXECUTION_DATE')
        3. Falls back to today minus 1 day (yesterday)
        
        Returns:
            LocalDate instance for the execution date
            
        Example:
            {%- set ds = ds() -%}
            {%- set start_quarter = ds.sub_months(2).start_of_quarter() -%}
    #}
    {%- set execution_date_str = var('EXECUTION_DATE', env_var('EXECUTION_DATE', "none")) -%}
    {%- if execution_date_str == 'none' -%}
        {# Default to yesterday if no EXECUTION_DATE is set #}
        {% do return(adapter.today().sub_days(1)) %}
    {%- else -%}
        {% do return(adapter.parse_date(execution_date_str)) %}
    {%- endif -%}
{%- endmacro -%}


{%- macro format_date(local_date, fmt='%Y-%m-%d') -%}
    {#
        Format a LocalDate as a string.
        
        Args:
            local_date: LocalDate instance
            fmt: Date format string (default: %Y-%m-%d)
            
        Returns:
            Formatted date string
            
        Example:
            {%- set ds = parse_date('2024-01-15') -%}
            {{ format_date(ds, '%Y%m%d') }}  -- Output: 20240115
    #}
    {% do return(local_date.format(fmt)) %}
{%- endmacro -%}


{%- macro date_range(start_date, end_date) -%}
    {#
        Generate a list of dates between start and end (inclusive).
        
        Args:
            start_date: Start LocalDate
            end_date: End LocalDate
            
        Returns:
            List of LocalDate instances
            
        Example:
            {%- set dates = date_range(parse_date('2024-01-01'), parse_date('2024-01-05')) -%}
            {%- for d in dates -%}
              '{{ d }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
            -- Output: '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
    #}
    {%- set dates = [] -%}
    {%- set current = start_date -%}
    {%- set days = start_date.days_between(end_date) -%}
    {%- for i in range(days + 1) -%}
        {%- do dates.append(start_date.add_days(i)) -%}
    {%- endfor -%}
    {% do return(dates) %}
{%- endmacro -%}


{%- macro months_between(start_date, end_date) -%}
    {#
        Calculate the number of months between two dates.
        
        Args:
            start_date: Start LocalDate
            end_date: End LocalDate
            
        Returns:
            Number of months (approximate, based on 30-day months)
    #}
    {%- set days = start_date.days_between(end_date) -%}
    {% do return((days / 30) | int) %}
{%- endmacro -%}
