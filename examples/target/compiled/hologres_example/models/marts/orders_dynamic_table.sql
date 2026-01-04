-- Example: Hologres Dynamic Table (物化视图)
-- This creates a materialized view with automatic refresh



select
    customer_id,
    count(*) as order_count,
    sum(amount) as total_spent,
    avg(amount) as avg_order_value,
    max(order_date) as last_order_date,
    clock_timestamp() as refreshed_at
from "from_dbt"."staging"."stg_orders"
group by customer_id