"""Integration tests for table operations in Hologres.

These tests require actual Hologres database connections and will:
- Create tables using dbt models
- Update table schemas
- Drop tables
- Test various table properties and configurations

Run with:
    DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true pytest tests/integration/test_table_operations.py
"""
import pytest
import os
from pathlib import Path


# Helper functions
def create_model_file(project_dir: Path, model_name: str, sql_content: str) -> Path:
    """Helper to create a model SQL file."""
    model_path = project_dir / "models" / f"{model_name}.sql"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with open(model_path, "w") as f:
        f.write(sql_content)
    return model_path


def create_seed_file(project_dir: Path, seed_name: str, csv_content: str) -> Path:
    """Helper to create a seed CSV file."""
    seed_path = project_dir / "seeds" / f"{seed_name}.csv"
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    with open(seed_path, "w") as f:
        f.write(csv_content)
    return seed_path


class TestTableCreation:
    """Tests for table creation operations."""

    def test_create_simple_table(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a simple table from a model."""
        # Create a simple model
        model_sql = """
select
    1 as id,
    'test' as name,
    100.0 as value
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "simple_table", model_sql)

        # Run dbt to create the table
        result = dbt_runner.invoke(["run"])
        assert result.success, f"dbt run failed: {result.exception}"

        # Verify the table was created
        result = dbt_runner.invoke(["list"])
        assert result.success
        output = str(result.result)
        assert "simple_table" in output

    def test_create_table_with_ctes(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a table using CTEs (Common Table Expressions)."""
        model_sql = """
with source_data as (
    select 1 as id, 'alice' as name, 25 as age
    union all
    select 2 as id, 'bob' as name, 30 as age
    union all
    select 3 as id, 'charlie' as name, 35 as age
),

transformed as (
    select
        id,
        upper(name) as name_upper,
        age * 2 as age_doubled
    from source_data
)

select * from transformed
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "cte_table", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success, f"dbt run failed: {result.exception}"

    def test_create_table_with_joins(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a table with JOIN operations."""
        # First create seed data
        users_csv = """id,name
1,Alice
2,Bob
3,Charlie
"""
        orders_csv = """order_id,user_id,amount
101,1,100.50
102,1,250.00
103,2,75.25
"""
        # create_seed_file is defined at module level
        create_seed_file(dbt_project_dir, "users", users_csv)
        create_seed_file(dbt_project_dir, "orders", orders_csv)

        # Run seeds to create tables
        result = dbt_runner.invoke(["seed"])
        assert result.success, f"dbt seed failed: {result.exception}"

        # Create model with JOIN
        model_sql = """
select
    u.id,
    u.name,
    count(o.order_id) as order_count,
    sum(o.amount) as total_amount
from {{ ref('users') }} u
left join {{ ref('orders') }} o on u.id = o.user_id
group by u.id, u.name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "user_orders", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success, f"dbt run failed: {result.exception}"

    def test_create_table_with_aggregations(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a table with aggregation functions."""
        model_sql = """
with source_data as (
    select generate_series(1, 100) as value
)
select
    'category1' as category,
    sum(value) as total_sum,
    avg(value)::integer as average_value,
    count(*) as record_count
from source_data
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "agg_table", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success, f"dbt run failed: {result.exception}"


class TestTableUpdates:
    """Tests for table update operations."""

    def test_add_column_to_table(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test adding a new column to an existing table."""
        # Create initial table
        model_sql = """
select
    1 as id,
    'test' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "expandable_table", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Update model to add a column
        updated_sql = """
select
    1 as id,
    'test' as name,
    100 as new_column
"""
        create_model_file(dbt_project_dir, "expandable_table", updated_sql)

        # Run with full-refresh to rebuild the table
        result = dbt_runner.invoke(["run", "--full-refresh"])
        assert result.success, f"dbt run --full-refresh failed: {result.exception}"

    def test_change_column_type(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test changing a column's data type."""
        model_sql = """
select
    1 as id,
    '100' as numeric_value
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "type_change_table", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Change column type
        updated_sql = """
select
    1 as id,
    100::int as numeric_value
"""
        create_model_file(dbt_project_dir, "type_change_table", updated_sql)

        result = dbt_runner.invoke(["run", "--full-refresh"])
        assert result.success

    def test_update_table_data(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test updating data in an existing table."""
        # Create seed data
        csv_data = """id,value
1,100
2,200
"""
        # create_seed_file is defined at module level
        create_seed_file(dbt_project_dir, "source_data", csv_data)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        # Create model
        model_sql = """
select * from {{ ref('source_data') }}
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "data_model", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Update seed data
        updated_csv = """id,value
1,150
2,250
3,300
"""
        create_seed_file(dbt_project_dir, "source_data", updated_csv)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        result = dbt_runner.invoke(["run"])
        assert result.success


class TestTableDeletion:
    """Tests for table deletion operations."""

    def test_drop_table(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test dropping a table."""
        model_sql = """
select 1 as id, 'test' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "drop_test_table", model_sql)

        # Create the table
        result = dbt_runner.invoke(["run"])
        assert result.success

        # Verify table exists
        result = dbt_runner.invoke(["list"])
        assert "drop_test_table" in str(result.result)

        # Delete the model file to simulate dropping
        model_path = dbt_project_dir / "models" / "drop_test_table.sql"
        model_path.unlink()

        # Run with empty models - the table should still exist since we don't drop by default
        # Instead, we verify the model is no longer in the list
        result = dbt_runner.invoke(["list"])
        # After removing the model file, it should not appear in the list
        assert "drop_test_table" not in str(result.result)

    def test_drop_multiple_tables(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test dropping multiple tables."""
        # Create multiple models
        for i in range(3):
            model_sql = f"select {i} as id, 'table{i}' as name"
            # create_model_file is defined at module level
            create_model_file(dbt_project_dir, f"multi_table_{i}", model_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Verify they exist
        result = dbt_runner.invoke(["list"])
        assert "multi_table_0" in str(result.result)
        assert "multi_table_1" in str(result.result)
        assert "multi_table_2" in str(result.result)

        # Remove all model files
        for i in range(3):
            model_path = dbt_project_dir / "models" / f"multi_table_{i}.sql"
            if model_path.exists():
                model_path.unlink()

        # Verify they're no longer in the list
        result = dbt_runner.invoke(["list"])
        assert "multi_table_0" not in str(result.result)


class TestTableProperties:
    """Tests for table properties and configurations."""

    def test_table_with_custom_schema(self, dbt_project_dir: Path, dbt_runner, unique_schema_name, cleanup_schema):
        """Test creating a table in a custom schema."""
        # Create a subdirectory model
        (dbt_project_dir / "models" / "custom_schema").mkdir()

        model_sql = """
select 1 as id, 'custom schema test' as name
"""
        model_path = dbt_project_dir / "models" / "custom_schema" / "custom_table.sql"
        with open(model_path, "w") as f:
            f.write(model_sql)

        # Configure custom schema in model properties
        model_config = """
version: 2

models:
  - name: custom_table
    schema: custom_schema_sub
"""
        config_path = dbt_project_dir / "models" / "custom_schema" / "schema.yml"
        with open(config_path, "w") as f:
            f.write(model_config)

        result = dbt_runner.invoke(["run"])
        assert result.success

    def test_table_with_materialization_config(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test table with different materialization configurations."""
        model_sql = """
select 1 as id, 'config test' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "config_table", model_sql)

        # Add configuration
        model_config = """
version: 2

models:
  - name: config_table
    config:
      materialized: table
      enabled: true
"""
        config_path = dbt_project_dir / "models" / "schema.yml"
        with open(config_path, "w") as f:
            f.write(model_config)

        result = dbt_runner.invoke(["run"])
        assert result.success


class TestIncrementalModels:
    """Tests for incremental model operations."""

    def test_incremental_model_initial_run(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test initial run of an incremental model."""
        # Create seed data
        csv_data = """id,name,created_at
1,Alice,2024-01-01
2,Bob,2024-01-02
"""
        # create_seed_file is defined at module level
        create_seed_file(dbt_project_dir, "source_data", csv_data)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        # Create incremental model
        incremental_sql = """
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select
    id,
    name,
    created_at
from {{ ref('source_data') }}

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{% endif %}
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "incremental_model", incremental_sql)

        result = dbt_runner.invoke(["run"])
        assert result.success

    def test_incremental_model_increment(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test incremental run adding new data."""
        # Initial seed
        csv_data = """id,name,created_at
1,Alice,2024-01-01
2,Bob,2024-01-02
"""
        # create_seed_file is defined at module level
        create_seed_file(dbt_project_dir, "source_data", csv_data)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        # Create incremental model
        incremental_sql = """
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select
    id,
    name,
    created_at
from {{ ref('source_data') }}

{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "incremental_model", incremental_sql)

        # Initial run
        result = dbt_runner.invoke(["run"])
        assert result.success

        # Add more data
        updated_csv = """id,name,created_at
1,Alice,2024-01-01
2,Bob,2024-01-02
3,Charlie,2024-01-03
4,David,2024-01-04
"""
        create_seed_file(dbt_project_dir, "source_data", updated_csv)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        # Incremental run
        result = dbt_runner.invoke(["run"])
        assert result.success


# Pytest markers for easy test selection
pytestmark = pytest.mark.integration
