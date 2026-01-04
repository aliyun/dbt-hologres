-- Example: Table with Hologres properties configured
-- This demonstrates how to configure table properties for optimal performance

{{config(
    materialized='table',
    schema='marts',
    orientation='column',
    distribution_key='order_id',
    clustering_key='order_date:asc',
    event_time_column='order_date',
    bitmap_columns='status,payment_method',
    dictionary_encoding_columns='customer_id'
)}}

-- Best Practices for Table Properties:
-- 1. orientation: Use 'column' for analytical queries, 'row' for transactional
-- 2. distribution_key: Choose frequently joined or grouped columns (prefer single column)
-- 3. clustering_key: Use for range queries (max 2 columns, left-match principle)
-- 4. event_time_column: Set for time-series data (timestamp columns)
-- 5. bitmap_columns: Use for equality filters (low cardinality, max 10 columns)
-- 6. dictionary_encoding_columns: Use for low cardinality string columns (max 10 columns)

select
    order_id,
    customer_id,
    order_date,
    status,
    payment_method,
    sum(amount) as total_amount,
    count(*) as item_count,
    min(amount) as min_amount,
    max(amount) as max_amount
from {{ ref('stg_orders') }}
group by 
    order_id, 
    customer_id, 
    order_date, 
    status, 
    payment_method
