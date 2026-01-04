-- Example: Incremental model with merge strategy
-- This model only processes new/updated records



select
    order_id,
    customer_id,
    amount,
    order_date,
    current_timestamp as updated_at
from "from_dbt"."staging"."stg_orders"


  -- Only include new or updated records
  where order_date > (select max(order_date) from "from_dbt"."marts"."orders_incremental")
