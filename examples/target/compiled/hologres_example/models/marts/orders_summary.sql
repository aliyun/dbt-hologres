-- Example: View model
-- This creates a view that summarizes orders by customer



select
    customer_id,
    count(*) as total_orders,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from "from_dbt"."staging"."stg_orders"
group by customer_id