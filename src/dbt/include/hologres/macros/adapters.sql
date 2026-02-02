{% macro hologres__get_create_table_as_sql(temporary, relation, sql) -%}
  {# Hologres需要在事务外执行CTAS #}
  {{ return(hologres__create_table_as(temporary, relation, sql)) }}
{%- endmacro %}

{% macro hologres__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {# Read table property configurations #}
    {%- set orientation = config.get('orientation', none) -%}
    {%- set distribution_key = config.get('distribution_key', none) -%}
    {%- set clustering_key = config.get('clustering_key', none) -%}
    {%- set event_time_column = config.get('event_time_column', none) -%}
    {%- set segment_key = config.get('segment_key', none) -%}
    {%- set bitmap_columns = config.get('bitmap_columns', none) -%}
    {%- set dictionary_encoding_columns = config.get('dictionary_encoding_columns', none) -%}
    {%- set logical_partition_key = config.get('logical_partition_key', none) -%}

    {# Handle segment_key as alias for event_time_column #}
    {%- if event_time_column is none and segment_key is not none -%}
      {%- set event_time_column = segment_key -%}
    {%- endif -%}

    {# Build WITH clause properties list #}
    {%- set with_properties = [] -%}
    {%- if orientation is not none -%}
      {%- do with_properties.append("orientation = '" ~ orientation ~ "'") -%}
    {%- endif -%}
    {%- if distribution_key is not none -%}
      {%- do with_properties.append("distribution_key = '" ~ distribution_key ~ "'") -%}
    {%- endif -%}
    {%- if clustering_key is not none -%}
      {%- do with_properties.append("clustering_key = '" ~ clustering_key ~ "'") -%}
    {%- endif -%}
    {%- if event_time_column is not none -%}
      {%- do with_properties.append("event_time_column = '" ~ event_time_column ~ "'") -%}
    {%- endif -%}
    {%- if bitmap_columns is not none -%}
      {%- do with_properties.append("bitmap_columns = '" ~ bitmap_columns ~ "'") -%}
    {%- endif -%}
    {%- if dictionary_encoding_columns is not none -%}
      {%- do with_properties.append("dictionary_encoding_columns = '" ~ dictionary_encoding_columns ~ "'") -%}
    {%- endif -%}

    {# Hologres不支持TEMPORARY TABLE，忽略temporary参数 #}
    {%- if logical_partition_key is not none -%}
      {# 逻辑分区表：此处仅返回普通CTAS，实际的逻辑分区表创建在materialization中处理 #}
      {# 这个分支仅用于向后兼容，实际逻辑分区表创建应通过hologres__create_logical_partition_table_from_ctas macro #}
      {% do exceptions.raise_compiler_error("逻辑分区表(logical_partition_key)需要使用table materialization，请确保materialized='table'") %}
    {%- else -%}
      {# 普通表使用CTAS #}
      create table {{ relation }}
      {%- if with_properties | length > 0 %}
      with (
        {{ with_properties | join(',\n      ') }}
      )
      {%- endif %}
      as (
        {{ compiled_code }}
      );
    {%- endif -%}
  {%- elif language == 'python' -%}
    {# Python models也不使用temporary #}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation, temporary=false) }}
  {%- else -%}
    {% do exceptions.raise_compiler_error("hologres__create_table_as macro didn't get supported language " ~ language) %}
  {%- endif -%}
{%- endmacro %}

{% macro hologres__get_column_defs_from_relation(temp_relation) -%}
  {#
    从现有表获取列定义，用于创建逻辑分区表
    返回格式: [{'column': 'col_name', 'dtype': 'data_type'}, ...]
  #}
  {%- set columns = adapter.get_columns_in_relation(temp_relation) -%}
  {{ return(columns) }}
{%- endmacro %}

{% macro hologres__build_column_definitions(columns, partition_columns) -%}
  {#
    构建列定义字符串，分区键列添加NOT NULL约束
    参数:
    - columns: 列信息列表
    - partition_columns: 分区键列名列表
  #}
  {%- set col_defs = [] -%}
  {%- for col in columns -%}
    {%- set col_name = col.column -%}
    {# 使用 data_type 属性获取完整类型定义（包含精度信息） #}
    {%- set col_type = col.data_type -%}
    {%- if col_name in partition_columns -%}
      {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
    {%- else -%}
      {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(col_defs | join(', ')) }}
{%- endmacro %}

{% macro hologres__create_logical_partition_table_ddl(relation, columns, logical_partition_key, with_properties) -%}
  {#
    生成创建逻辑分区表的DDL语句
    参数:
    - relation: 目标表
    - columns: 列信息列表
    - logical_partition_key: 分区键，支持1-2个列，用逗号分隔
    - with_properties: WITH子句属性列表
  #}
  {%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
  {%- set column_defs = hologres__build_column_definitions(columns, partition_columns) -%}
  
  create table {{ relation }} (
    {{ column_defs }}
  )
  logical partition by list ({{ partition_columns | join(', ') }})
  {%- if with_properties | length > 0 %}
  with (
    {{ with_properties | join(',\n    ') }}
  )
  {%- endif %};
{%- endmacro %}

{% macro hologres__insert_into_table(relation, compiled_code) -%}
  {# 将数据插入到表中 #}
  insert into {{ relation }}
  {{ compiled_code }};
{%- endmacro %}

{% macro hologres__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{%- endmacro %}

{% macro hologres__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
        '{{ database }}' as table_database,
        sch.nspname as table_schema,
        tbl.relname as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            when 'r' then 'BASE TABLE'
        end as table_type,
        tbl_desc.description as table_comment,
        col.attname as column_name,
        col.attnum as column_index,
        pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
        col_desc.description as column_comment,
        pg_catalog.col_description(tbl.oid, col.attnum) as column_description,
        '' as table_owner
    from pg_catalog.pg_class tbl
    inner join pg_catalog.pg_namespace sch on tbl.relnamespace = sch.oid
    inner join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
    left outer join pg_catalog.pg_description tbl_desc on (
            tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0
    )
    left outer join pg_catalog.pg_description col_desc on (
            col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum
    )
    where (
        {%- for schema in schemas -%}
          upper(sch.nspname) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
      and tbl.relkind in ('r', 'v', 'm', 'f', 'p')
      and col.attnum > 0
      and not col.attisdropped
    order by sch.nspname, tbl.relname, col.attnum
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}

{% macro hologres__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro hologres__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct nspname from pg_namespace
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro hologres__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
        select count(*) from pg_namespace where nspname = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro hologres__create_schema(relation) -%}
  {% call statement('create_schema') %}
    create schema if not exists {{ relation.without_identifier().include(database=False) }}
  {% endcall %}
{% endmacro %}

{% macro hologres__drop_schema(relation) -%}
  {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
  {% endcall %}
{% endmacro %}

{% macro hologres__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro hologres__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro hologres__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    {{ get_rename_sql(from_relation, target_name) }}
  {%- endcall %}
{% endmacro %}

{% macro hologres__get_rename_view_sql(relation, new_name) %}
    {# Hologres does not support database-qualified names in ALTER VIEW RENAME #}
    alter view {{ relation.include(database=False) }} rename to {{ new_name }}
{% endmacro %}

{% macro hologres__get_rename_table_sql(relation, new_name) %}
    {# Hologres does not support database-qualified names in ALTER TABLE RENAME #}
    alter table {{ relation.include(database=False) }} rename to {{ new_name }}
{% endmacro %}

{% macro hologres__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from information_schema.columns
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro hologres__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro hologres__get_relations() %}
  {# Hologres-specific macro to get relation dependencies #}
  {% call statement('get_relations', fetch_result=True) %}
      select
          dependent_ns.nspname as dependent_schema,
          dependent_view.relname as dependent_name,
          source_ns.nspname as referenced_schema,
          source_table.relname as referenced_name
      from pg_depend
      join pg_rewrite on pg_depend.objid = pg_rewrite.oid
      join pg_class as dependent_view on pg_rewrite.ev_class = dependent_view.oid
      join pg_class as source_table on pg_depend.refobjid = source_table.oid
      join pg_namespace dependent_ns on dependent_ns.oid = dependent_view.relnamespace
      join pg_namespace source_ns on source_ns.oid = source_table.relnamespace
      where dependent_view.relkind = 'v'
        and source_table.relkind = 'r'
        and pg_depend.deptype = 'n'
        and dependent_ns.nspname != source_ns.nspname
  {% endcall %}

  {% do return(load_result('get_relations').table) %}
{% endmacro %}
