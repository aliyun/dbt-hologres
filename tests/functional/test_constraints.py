"""Functional tests for constraint support in dbt-hologres.

These tests verify that database constraints are properly enforced
when creating tables with dbt models.

Constraint types tested:
- not_null: Column must not contain NULL values
- unique: Column values must be unique
- primary_key: Primary key constraint
- foreign_key: Foreign key reference to another table
- check: Custom check constraint

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_constraints.py
"""
import pytest

from dbt.tests.util import run_dbt


class TestNotNullConstraint:
    """Tests for NOT NULL constraint enforcement."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with NOT NULL constraint on id column."""
        return {
            "not_null_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    'name_' || i as name
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: not_null_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: not_null
      - name: name
        data_type: text
""",
        }

    def test_not_null_constraint_creates_table(self, project):
        """Test that table with NOT NULL constraint is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestUniqueConstraint:
    """Tests for UNIQUE constraint enforcement."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with UNIQUE constraint on id column."""
        return {
            "unique_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    'value_' || i as name
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: unique_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: unique
      - name: name
        data_type: text
""",
        }

    def test_unique_constraint_creates_table(self, project):
        """Test that table with UNIQUE constraint is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestPrimaryKeyConstraint:
    """Tests for PRIMARY KEY constraint enforcement."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with PRIMARY KEY constraint on id column."""
        return {
            "pk_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    'user_' || i as username
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: pk_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: primary_key
      - name: username
        data_type: text
""",
        }

    def test_primary_key_constraint_creates_table(self, project):
        """Test that table with PRIMARY KEY constraint is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestForeignKeyConstraint:
    """Tests for FOREIGN KEY constraint enforcement."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define parent and child tables with FOREIGN KEY relationship."""
        return {
            "parent_table.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    'parent_' || i as name
from generate_series(1, 10) as s(i)
""",
            "child_table.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    (i % 10) + 1 as parent_id,
    'child_' || i as name
from generate_series(1, 20) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: parent_table
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: primary_key
      - name: name
        data_type: text
  - name: child_table
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: parent_id
        data_type: integer
        constraints:
          - type: foreign_key
            to: parent_table
            to_columns: [id]
      - name: name
        data_type: text
""",
        }

    def test_foreign_key_constraint_creates_tables(self, project):
        """Test that tables with FOREIGN KEY relationship are created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 2
        for result in results:
            assert result.status == "success"


class TestCheckConstraint:
    """Tests for CHECK constraint enforcement."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with CHECK constraint on value column."""
        return {
            "check_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    i * 100 as value
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: check_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: value
        data_type: integer
        constraints:
          - type: check
            expression: value > 0
""",
        }

    def test_check_constraint_creates_table(self, project):
        """Test that table with CHECK constraint is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestMultipleConstraintsOnSingleColumn:
    """Tests for multiple constraints on a single column."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with multiple constraints on id column."""
        return {
            "multi_constraint_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    'item_' || i as name,
    i * 10 as value
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: multi_constraint_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: not_null
          - type: unique
      - name: name
        data_type: text
        constraints:
          - type: not_null
      - name: value
        data_type: integer
        constraints:
          - type: check
            expression: value >= 10
""",
        }

    def test_multiple_constraints_creates_table(self, project):
        """Test that table with multiple constraints is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestCompositePrimaryKey:
    """Tests for composite primary key constraint."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with composite primary key."""
        return {
            "composite_pk_model.sql": """
{{ config(
    materialized='table'
) }}

select
    i as user_id,
    (i % 5) as group_id,
    'member_' || i as role
from generate_series(1, 20) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: composite_pk_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [user_id, group_id]
    columns:
      - name: user_id
        data_type: integer
        constraints:
          - type: not_null
      - name: group_id
        data_type: integer
        constraints:
          - type: not_null
      - name: role
        data_type: text
""",
        }

    def test_composite_primary_key_creates_table(self, project):
        """Test that table with composite primary key is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestModelLevelConstraint:
    """Tests for model-level constraints."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define model with model-level check constraint."""
        return {
            "model_level_constraint.sql": """
{{ config(
    materialized='table'
) }}

select
    i as id,
    i * 100 as min_value,
    i * 200 as max_value
from generate_series(1, 10) as s(i)
""",
            "schema.yml": """
version: 2
models:
  - name: model_level_constraint
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: max_value > min_value
    columns:
      - name: id
        data_type: integer
      - name: min_value
        data_type: integer
      - name: max_value
        data_type: integer
""",
        }

    def test_model_level_constraint_creates_table(self, project):
        """Test that table with model-level constraint is created successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
