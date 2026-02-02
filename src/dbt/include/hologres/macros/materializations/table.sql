{% materialization table, adapter='hologres' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}
  
  {# 检查是否是逻辑分区表 #}
  {%- set logical_partition_key = config.get('logical_partition_key', none) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- if logical_partition_key is not none -%}
    {# 
      逻辑分区表创建流程 (Hologres不支持CTAS创建逻辑分区表):
      1. 用CTAS创建临时表获取列结构 (LIMIT 0)
      2. 从临时表获取列定义
      3. 创建带 LOGICAL PARTITION BY LIST 的目标表
      4. 插入数据
      5. 清理临时表
    #}
    {%- set temp_for_schema = make_temp_relation(intermediate_relation, '_schema') -%}
    
    -- Step 1: 创建临时表获取列结构
    {% call statement('create_temp_for_schema', auto_begin=False) -%}
      create table {{ temp_for_schema }} as ({{ sql }}) limit 0
    {%- endcall %}
    
    -- Step 2: 获取列定义
    {%- set columns = adapter.get_columns_in_relation(temp_for_schema) -%}
    
    -- Step 3: 删除临时表
    {% call statement('drop_temp_for_schema', auto_begin=False) -%}
      drop table if exists {{ temp_for_schema }}
    {%- endcall %}
    
    -- Step 4: 构建WITH属性
    {%- set orientation = config.get('orientation', none) -%}
    {%- set distribution_key = config.get('distribution_key', none) -%}
    {%- set clustering_key = config.get('clustering_key', none) -%}
    {%- set event_time_column = config.get('event_time_column', none) -%}
    {%- set segment_key = config.get('segment_key', none) -%}
    {%- set bitmap_columns = config.get('bitmap_columns', none) -%}
    {%- set dictionary_encoding_columns = config.get('dictionary_encoding_columns', none) -%}
    
    {%- if event_time_column is none and segment_key is not none -%}
      {%- set event_time_column = segment_key -%}
    {%- endif -%}
    
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
    
    -- Step 5: 创建逻辑分区表
    {% call statement('create_logical_partition_table', auto_begin=False) -%}
      {{ hologres__create_logical_partition_table_ddl(intermediate_relation, columns, logical_partition_key, with_properties) }}
    {%- endcall %}
    
    -- Step 6: 插入数据
    {% call statement('main', auto_begin=False) -%}
      {{ hologres__insert_into_table(intermediate_relation, sql) }}
    {%- endcall %}
    
  {%- else -%}
    -- build model (Hologres需要在事务外执行CTAS)
    -- 由于Hologres的add_begin_query被禁用，实际上不会开启事务
    {% call statement('main', auto_begin=False) -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
  {%- endif -%}

  {% do create_indexes(intermediate_relation) %}

  -- cleanup
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
