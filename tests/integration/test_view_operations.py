"""Integration tests for view operations in Hologres.

These tests test view creation, updates, and dependencies.

Run with:
    DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true pytest tests/integration/test_view_operations.py
"""
import pytest
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


class TestViewCreation:
    """Tests for view creation operations."""

    def test_create_simple_view(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a simple view."""
        # Create a base model first
        base_model = """
select
    1 as id,
    'test' as name,
    100.0 as value
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "base_model", base_model)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Create a view on top
        view_sql = """
select
    id,
    name,
    value * 1.1 as value_with_tax
from {{ ref('base_model') }}
"""
        create_model_file(dbt_project_dir, "simple_view", view_sql)

        # Create schema.yml to configure as view
        schema_yml = """
version: 2

models:
  - name: simple_view
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success, f"dbt run failed: {result.exception}"

    def test_view_with_aggregations(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a view with aggregations."""
        # Create base model
        base_model = """
select
    1 as id,
    'alice' as name,
    'sales' as department,
    50000 as salary
union all
select 2, 'bob', 'engineering', 75000
union all
select 3, 'charlie', 'sales', 55000
union all
select 4, 'diana', 'engineering', 80000
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "employees", base_model)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Create aggregation view
        view_sql = """
select
    department,
    count(*) as employee_count,
    avg(salary) as avg_salary,
    sum(salary) as total_salary
from {{ ref('employees') }}
group by department
"""
        create_model_file(dbt_project_dir, "department_summary", view_sql)

        schema_yml = """
version: 2

models:
  - name: department_summary
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

    def test_view_with_joins(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a view with JOIN operations."""
        # Create seed data
        users_csv = """id,name
1,Alice
2,Bob
"""
        orders_csv = """order_id,user_id,amount
101,1,100
102,2,200
"""
        # create_seed_file is defined at module level
        create_seed_file(dbt_project_dir, "users", users_csv)
        create_seed_file(dbt_project_dir, "orders", orders_csv)

        result = dbt_runner.invoke(["seed"])
        assert result.success

        # Create view with JOIN
        view_sql = """
select
    u.name,
    count(o.order_id) as order_count,
    sum(o.amount) as total_spent
from {{ ref('users') }} u
left join {{ ref('orders') }} o on u.id = o.user_id
group by u.id, u.name
"""
        create_model_file(dbt_project_dir, "user_order_view", view_sql)

        schema_yml = """
version: 2

models:
  - name: user_order_view
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success


class TestViewDependencies:
    """Tests for view dependency management."""

    def test_view_chain(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test creating a chain of views depending on each other."""
        # Create base table
        base_model = """
select 1 as id, 'base' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "base_table", base_model)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Create first view
        view1_sql = """
select id, upper(name) as name_upper
from {{ ref('base_table') }}
"""
        create_model_file(dbt_project_dir, "view_level_1", view1_sql)

        schema_yml = """
version: 2

models:
  - name: view_level_1
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Create second view depending on first
        view2_sql = """
select id, name_upper, length(name_upper) as name_length
from {{ ref('view_level_1') }}
"""
        create_model_file(dbt_project_dir, "view_level_2", view2_sql)

        schema_yml = """
version: 2

models:
  - name: view_level_1
    config:
      materialized: view
  - name: view_level_2
    config:
      materialized: view
"""
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

    def test_view_update_cascade(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test that updating base model cascades to dependent views."""
        # Create base model
        base_model = """
select 1 as id, 'original' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "base_data", base_model)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Create view
        view_sql = """
select id, name from {{ ref('base_data') }}
"""
        create_model_file(dbt_project_dir, "dependent_view", view_sql)

        schema_yml = """
version: 2

models:
  - name: dependent_view
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Update base model
        updated_model = """
select 1 as id, 'updated' as name
"""
        create_model_file(dbt_project_dir, "base_data", updated_model)

        result = dbt_runner.invoke(["run"])
        assert result.success


class TestViewOperations:
    """Tests for view-specific operations."""

    def test_view_to_table_conversion(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test converting a view to a table."""
        # Create as view
        view_sql = """
select 1 as id, 'test' as name
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "convertible_model", view_sql)

        schema_yml = """
version: 2

models:
  - name: convertible_model
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

        # Convert to table
        schema_yml = """
version: 2

models:
  - name: convertible_model
    config:
      materialized: table
"""
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success

    def test_view_with_complex_logic(self, dbt_project_dir: Path, dbt_runner, cleanup_schema):
        """Test view with complex SQL logic."""
        complex_view = """
with
series as (
    select generate_series(1, 10) as n
),

calculated as (
    select
        n,
        n * 2 as doubled,
        n ^ 2 as squared,
        n % 3 as mod_three
    from series
)

select
    n,
    doubled,
    squared,
    case
        when mod_three = 0 then 'divisible'
        else 'not divisible'
    end as divisible_by_three
from calculated
"""
        # create_model_file is defined at module level
        create_model_file(dbt_project_dir, "complex_view", complex_view)

        schema_yml = """
version: 2

models:
  - name: complex_view
    config:
      materialized: view
"""
        schema_path = dbt_project_dir / "models" / "schema.yml"
        with open(schema_path, "w") as f:
            f.write(schema_yml)

        result = dbt_runner.invoke(["run"])
        assert result.success


pytestmark = pytest.mark.integration
