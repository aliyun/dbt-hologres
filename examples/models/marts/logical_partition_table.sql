-- Example: Logical Partition Table (Single Partition Key)
-- This demonstrates how to create a logical partition table in Hologres
-- Logical partitions allow efficient data management and query optimization

{{ config(
    materialized='table',
    schema='marts',
    orientation='column',
    distribution_key='order_id',
    logical_partition_key='ds'
) }}

-- Logical Partition Table Requirements:
-- 1. Supports 1-2 partition columns
-- 2. Supported types: INT, TEXT, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ
-- 3. Partition keys are automatically set to NOT NULL
-- 4. Single column example: logical_partition_key='ds'
-- 5. Two columns example: logical_partition_key='yy,mm'

-- Common WITH options for logical partition tables:
-- - partition_expiration_time: Auto-delete partitions after specified seconds
-- - partition_keep_hot_window: Keep recent partitions in hot storage
-- - partition_require_filter: Require partition filter in queries

select
    order_id,
    customer_id,
    cast(order_date as date) as ds,
    cast(sum(amount) as numeric(12,2)) as total_amount,
    count(*) as item_count
from {{ ref('stg_orders') }}
group by 
    order_id, 
    customer_id, 
    order_date
