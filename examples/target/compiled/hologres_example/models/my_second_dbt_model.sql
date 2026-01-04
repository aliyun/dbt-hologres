-- Use the `ref` function to select from other models

select *
from "from_dbt"."public"."my_first_dbt_model"
where id = 1