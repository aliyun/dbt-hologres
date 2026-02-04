{{ 
    config(
        materialized='table',
        sql_header="SET TIME ZONE 'UTC';"
    ) 
}}

SELECT current_setting('timezone')