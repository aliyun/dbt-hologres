{% macro hologres__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {#
        Hologres uses PostgreSQL-compatible INSERT ON CONFLICT syntax instead of MERGE INTO.
        This macro implements the merge strategy using INSERT ON CONFLICT DO UPDATE.

        Args:
            target: The target relation to merge into
            source: The source relation (temp table) to merge from
            unique_key: The unique key column(s) for conflict detection
            dest_columns: The columns to insert/update
            incremental_predicates: Optional additional predicates (not used in this implementation)

        Returns:
            SQL statement for INSERT ON CONFLICT
    #}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

    {%- set unique_key_list = [] -%}
    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% do unique_key_list.append(key) %}
            {% endfor %}
        {% else %}
            {% do unique_key_list.append(unique_key) %}
        {% endif %}
    {% endif %}

    {%- set conflict_columns = unique_key_list | join(', ') -%}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
    {% if unique_key %}
    on conflict ({{ conflict_columns }})
    do update set
        {% for column_name in update_columns -%}
            {{ column_name }} = excluded.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% else %}
    -- No unique key, just insert (append behavior)
    on conflict do nothing
    {% endif %}

{%- endmacro %}


{% macro hologres__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {#
        Hologres implementation of delete+insert strategy.
        This deletes matching records and inserts new ones.
    #}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is string %}
        {% set unique_key = [unique_key] %}
        {% endif %}

        {%- set unique_key_str = unique_key|join(', ') -%}

        delete from {{ target }} as DBT_INTERNAL_DEST
        where ({{ unique_key_str }}) in (
            select distinct {{ unique_key_str }}
            from {{ source }} as DBT_INTERNAL_SOURCE
        )
        {%- if incremental_predicates %}
            {% for predicate in incremental_predicates %}
                and {{ predicate }}
            {% endfor %}
        {%- endif -%};

    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}