{{
    config(
        materialized='table',
        tags=['demo', 'date_utils']
    )
}}

{#
    Demo: LocalDate Usage Example
    
    This model demonstrates how to use the parse_date() function and LocalDate class
    for date manipulation in dbt models.
    
    Run with custom execution date:
        dbt run --select date_utils_demo --vars '{"EXECUTION_DATE": "2024-03-15"}'
    
    Or set environment variable:
        export EXECUTION_DATE=2024-03-15
        dbt run --select date_utils_demo
#}

{# Get execution date using ds() macro #}
{%- set ds = ds() -%}

{# Calculate various date ranges #}
{%- set start_quarter = ds.sub_months(2).start_of_quarter() -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{%- set end_date = ds.start_of_month() -%}

{# Year-over-year comparison dates #}
{%- set start_date_last = start_date.sub_months(12) -%}
{%- set end_date_last = end_date.sub_months(12) -%}

{# Additional date calculations #}
{%- set current_quarter_start = ds.start_of_quarter() -%}
{%- set current_quarter_end = ds.end_of_quarter() -%}
{%- set year_start = ds.start_of_year() -%}
{%- set last_week = ds.sub_days(7) -%}

-- =====================================================
-- LocalDate Demo: Date Calculations
-- =====================================================
-- Execution Date (ds): {{ ds }}
-- 
-- Calculated Date Ranges:
-- -----------------------
-- Start Quarter:        {{ start_quarter }}
-- Start Date:           {{ start_date }}
-- End Date:             {{ end_date }}
-- 
-- Year-over-Year Comparison:
-- --------------------------
-- Start Date Last Year: {{ start_date_last }}
-- End Date Last Year:   {{ end_date_last }}
-- 
-- Additional Calculations:
-- ------------------------
-- Current Quarter Start: {{ current_quarter_start }}
-- Current Quarter End:   {{ current_quarter_end }}
-- Year Start:            {{ year_start }}
-- Last Week:             {{ last_week }}
-- =====================================================

SELECT
    -- Current period dates
    '{{ ds }}' as execution_date,
    '{{ start_quarter }}' as start_quarter,
    '{{ start_date }}' as start_date,
    '{{ end_date }}' as end_date,
    
    -- Year-over-year dates
    '{{ start_date_last }}' as start_date_last_year,
    '{{ end_date_last }}' as end_date_last_year,
    
    -- Quarter dates
    '{{ current_quarter_start }}' as current_quarter_start,
    '{{ current_quarter_end }}' as current_quarter_end,
    
    -- Other calculations
    '{{ year_start }}' as year_start,
    '{{ last_week }}' as last_week,
    
    -- Date properties
    {{ ds.year }} as ds_year,
    {{ ds.month }} as ds_month,
    {{ ds.day }} as ds_day,
    {{ ds.quarter }} as ds_quarter,
    
    -- Days between calculation
    {{ start_date.days_between(end_date) }} as days_in_period,
    
    -- Formatted dates (different formats)
    '{{ ds.format('%Y%m%d') }}' as ds_compact,
    '{{ ds.format('%Y/%m/%d') }}' as ds_slash
