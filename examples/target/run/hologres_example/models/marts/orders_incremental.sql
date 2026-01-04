
      insert into "from_dbt"."marts"."orders_incremental" ("order_id", "customer_id", "amount", "order_date", "updated_at")
    (
        select "order_id", "customer_id", "amount", "order_date", "updated_at"
        from "from_dbt"."marts"."orders_incremental__dbt_tmp20251230160231040529"
    )


  