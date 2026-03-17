"""Shared test fixtures for dbt-hologres functional tests.

This module provides reusable test models and seed data for functional tests.
Each fixture defines the model/seed name and its content.

Usage in test classes:
    class TestMyFeature:
        @pytest.fixture(scope="class")
        def models(self):
            return {
                "my_model.sql": models__simple_model,
            }

        @pytest.fixture(scope="class")
        def seeds(self):
            return {
                "my_seed.csv": seeds__sample_data,
            }
"""
from typing import Dict

# =============================================================================
# Model Fixtures
# =============================================================================

# Simple model with basic SELECT
models__simple_model = """
{{ config(materialized='table') }}

select
    1 as id,
    'test' as name,
    current_timestamp as created_at
"""

# Model with table materialization and indexes
# Note: Hologres doesn't support random() function (Single Row Volatile functions)
models__table_with_index = """
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['user_id'], 'type': 'bitmap'}
    ]
) }}

select
    generate_series(1, 100) as id,
    (i % 100) * 10 as user_id,
    'event_' || generate_series(1, 100) as event_name
from generate_series(1, 100) as s(i)
"""

# Model with table materialization and Hologres properties
# Note: Hologres doesn't support random() function (Single Row Volatile functions)
models__table_with_properties = """
{{ config(
    materialized='table',
    orientation='column',
    distribution_key='user_id',
    lifecycle=30
) }}

select
    (i % 100) * 10 as user_id,
    (i % 10) * 10 as product_id,
    (i % 1000)::bigint as amount,
    current_timestamp as created_at
from generate_series(1, 1000) as s(i)
"""

# Incremental model with append strategy
models__incremental_append = """
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
    i as id,
    'value_' || i as name
from generate_series(1, 10) as s(i)

{% if is_incremental() %}
where i > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
"""

# Incremental model with merge strategy
models__incremental_merge = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
) }}

select
    i as id,
    'updated_' || i as name
from generate_series(1, 10) as s(i)
"""

# Dynamic table with basic configuration
models__dynamic_table_basic = """
{{ config(
    materialized='dynamic_table',
    target_lag='30 minutes'
) }}

select
    user_id,
    count(*) as event_count,
    sum(amount) as total_amount
from {{ ref('source_events') }}
group by user_id
"""

# Dynamic table with auto-refresh
models__dynamic_table_auto_refresh = """
{{ config(
    materialized='dynamic_table',
    target_lag='5 minutes',
    auto_refresh=true
) }}

select
    category,
    count(*) as item_count,
    avg(value)::integer as avg_value
from {{ ref('source_items') }}
group by category
"""

# Logical partition table with single key
models__logical_partition_single = """
{{ config(
    materialized='table',
    logical_partition_key='ds'
) }}

select
    current_date - (i || ' days')::interval as ds,
    i as id,
    'data_' || i as name
from generate_series(0, 29) as s(i)
"""

# Logical partition table with multiple keys
models__logical_partition_multiple = """
{{ config(
    materialized='table',
    logical_partition_key='yy,mm'
) }}

select
    extract(year from current_date - (i || ' days')::interval)::text as yy,
    extract(month from current_date - (i || ' days')::interval)::text as mm,
    i as id
from generate_series(0, 60) as s(i)
"""

# Model with clustering keys
# Note: Hologres doesn't support random() function (Single Row Volatile functions)
models__table_with_clustering_keys = """
{{ config(
    materialized='table',
    clustering_keys=['event_time', 'user_id']
) }}

select
    current_timestamp - (i || ' days')::interval as event_time,
    (i % 100) * 10 as user_id,
    'event_' || i as event_name
from generate_series(1, 100) as s(i)
"""

# Model using date utils macro
models__with_date_utils = """
{{ config(materialized='table') }}

select
    {{ local_date('2024-01-15').to_sql() }} as base_date,
    {{local_date('2024-01-15').sub_days(7).to_sql() }} as week_ago,
    {{local_date('2024-01-15').start_of_month().to_sql() }} as month_start,
    {{local_date('2024-01-15').end_of_month().to_sql() }} as month_end
"""

# Model with dependencies (downstream)
models__dependent_model = """
select
    id,
    name,
    created_at
from {{ ref('simple_model') }}
where id is not null
"""

# View model fixtures
models__basic_view = """
{{ config(materialized='view') }}

select
    1 as id,
    'test' as name,
    current_timestamp as created_at
"""

# Model with sql_header for GUC settings
models__with_sql_header = """
{{ config(
    materialized='table',
    sql_header="SET TIME ZONE 'UTC';"
) }}

select
    generate_series(1, 10) as id,
    'test' as name,
    current_timestamp as created_at
"""

# Incremental model with on_schema_change
models__incremental_schema_change = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

select
    i as id,
    'name_' || i as name,
    i * 100 as value
from generate_series(1, 10) as s(i)
"""

# Table with event_time_column
models__table_with_event_time = """
{{ config(
    materialized='table',
    event_time_column='event_time'
) }}

select
    current_timestamp - (i || ' days')::interval as event_time,
    i as id,
    'event_' || i as name
from generate_series(1, 50) as s(i)
"""

# Table with bitmap columns
models__table_with_bitmap_columns = """
{{ config(
    materialized='table',
    bitmap_columns='user_id,status',
    orientation='column'
) }}

select
    i as id,
    (i % 100) as user_id,
    case (i % 3) when 0 then 'active' when 1 then 'pending' else 'inactive' end as status
from generate_series(1, 100) as s(i)
"""

# Table with dictionary encoding columns
models__table_with_dict_encoding = """
{{ config(
    materialized='table',
    dictionary_encoding_columns='category,region',
    orientation='column'
) }}

select
    i as id,
    case (i % 3) when 0 then 'A' when 1 then 'B' else 'C' end as category,
    case (i % 4) when 0 then 'North' when 1 then 'South' when 2 then 'East' else 'West' end as region
from generate_series(1, 50) as s(i)
"""

# =============================================================================
# Seed Fixtures
# =============================================================================

# Sample event data
seeds__source_events = """user_id,amount,event_time
1,100.50,2024-01-01 10:00:00
1,250.00,2024-01-01 11:00:00
2,75.25,2024-01-01 12:00:00
2,150.00,2024-01-01 13:00:00
3,200.00,2024-01-02 09:00:00
"""

# Sample item data
seeds__source_items = """category,value,created_at
A,100,2024-01-01
A,150,2024-01-02
B,200,2024-01-01
B,250,2024-01-02
C,300,2024-01-03
"""

# Basic sample data for simple tests
seeds__sample_data = """id,name,value
1,alpha,100
2,beta,200
3,gamma,300
"""

# Data for incremental tests
seeds__incremental_base = """id,name,updated_at
1,initial_1,2024-01-01
2,initial_2,2024-01-01
3,initial_3,2024-01-01
"""


# =============================================================================
# Helper Functions
# =============================================================================

def get_model_fixtures() -> Dict[str, str]:
    """Get all model fixtures as a dictionary.

    Returns:
        Dict mapping model names to their SQL content
    """
    return {
        "simple_model.sql": models__simple_model,
        "table_with_index.sql": models__table_with_index,
        "table_with_properties.sql": models__table_with_properties,
        "incremental_append.sql": models__incremental_append,
        "incremental_merge.sql": models__incremental_merge,
        "dynamic_table_basic.sql": models__dynamic_table_basic,
        "dynamic_table_auto_refresh.sql": models__dynamic_table_auto_refresh,
        "logical_partition_single.sql": models__logical_partition_single,
        "logical_partition_multiple.sql": models__logical_partition_multiple,
        "table_with_clustering_keys.sql": models__table_with_clustering_keys,
        "with_date_utils.sql": models__with_date_utils,
        "dependent_model.sql": models__dependent_model,
        "basic_view.sql": models__basic_view,
        "with_sql_header.sql": models__with_sql_header,
        "incremental_schema_change.sql": models__incremental_schema_change,
        "table_with_event_time.sql": models__table_with_event_time,
        "table_with_bitmap_columns.sql": models__table_with_bitmap_columns,
        "table_with_dict_encoding.sql": models__table_with_dict_encoding,
    }


def get_seed_fixtures() -> Dict[str, str]:
    """Get all seed fixtures as a dictionary.

    Returns:
        Dict mapping seed names to their CSV content
    """
    return {
        "source_events.csv": seeds__source_events,
        "source_items.csv": seeds__source_items,
        "sample_data.csv": seeds__sample_data,
        "incremental_base.csv": seeds__incremental_base,
    }