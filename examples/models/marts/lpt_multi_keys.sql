-- Example: Logical Partition Table with Multiple Partition Keys (lpt_multi_keys)
-- This demonstrates how to create a logical partition table with 2 partition columns
-- Useful for hierarchical time-based partitioning (e.g., year + month)
-- Note: Model name shortened to avoid 63-character PostgreSQL identifier limit

{{ config(
    materialized='table',
    schema='marts',
    orientation='column',
    distribution_key='order_id',
    logical_partition_key='order_year, order_month'
) }}

-- Multi-column Logical Partition Table:
-- - Supports up to 2 partition columns
-- - Columns are separated by comma in logical_partition_key
-- - Both columns are automatically set to NOT NULL
-- - Common use cases: year+month, region+date, category+date

-- Example partition query optimization:
-- SELECT * FROM table WHERE order_year = 2024 AND order_month = 1
-- This will only scan the specific partition

select
    order_id,
    customer_id,
    extract(year from order_date)::int as order_year,
    extract(month from order_date)::int as order_month,
    cast(sum(amount) as numeric(12,2)) as total_amount,
    count(*) as item_count
from {{ ref('stg_orders') }}
group by 
    order_id, 
    customer_id, 
    order_date
