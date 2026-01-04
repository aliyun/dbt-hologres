-- Example: Simple table model
-- This creates a standard table in Hologres
--
-- To configure table properties for better performance, you can add:
--   orientation='column',          -- Storage format: column/row/row,column
--   distribution_key='order_id',   -- Distribution key for data sharding
--   clustering_key='order_date:asc', -- Clustering index for range queries
--   event_time_column='order_date',  -- Time-based segmentation (alias: segment_key)
--   bitmap_columns='status',       -- Bitmap index for equality queries
--   dictionary_encoding_columns='customer_id' -- Dictionary encoding for low cardinality
--
-- See table_with_properties.sql for a complete example

{{ config(
    materialized='table',
    schema='staging'
) }}

select
    1 as order_id,
    'customer_1' as customer_id,
    cast(100.50 as numeric(10,2)) as amount,
    current_timestamp as order_date

union all

select
    2 as order_id,
    'customer_2' as customer_id,
    cast(250.75 as numeric(10,2)) as amount,
    current_timestamp as order_date

union all

select
    3 as order_id,
    'customer_1' as customer_id,
    cast(75.25 as numeric(10,2)) as amount,
    current_timestamp as order_date
