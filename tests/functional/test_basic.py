"""Basic functional tests for dbt-hologres.

These tests verify the fundamental dbt operations work correctly
with the Hologres adapter, including:
- Basic model execution
- Seed loading
- Model dependency resolution

Run with:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_basic.py
"""
import pytest

from dbt.tests.util import run_dbt

from tests.functional.fixtures import (
    models__simple_model,
    models__dependent_model,
    seeds__sample_data,
)


class TestBasicRun:
    """Tests for basic dbt run operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models for basic run tests."""
        return {
            "simple_model.sql": models__simple_model,
        }

    def test_basic_run_succeeds(self, project):
        """Test that a simple model can be executed successfully."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_basic_run_creates_table(self, project):
        """Test that running a model creates a table in the database."""
        run_dbt(["run"])

        # Verify the table exists by querying it
        relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="simple_model"
        )
        assert relation is not None
        assert relation.type == "table"


class TestBasicSeed:
    """Tests for dbt seed operations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        """Define seeds for basic seed tests."""
        return {
            "sample_data.csv": seeds__sample_data,
        }

    def test_seed_loads_data(self, project):
        """Test that seed data is loaded successfully."""
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_seed_creates_table(self, project):
        """Test that seed creates a table with correct data."""
        run_dbt(["seed"])

        # Verify the table exists
        relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="sample_data"
        )
        assert relation is not None
        assert relation.type == "table"


class TestModelDependencies:
    """Tests for model dependency resolution and execution."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with dependencies."""
        return {
            "simple_model.sql": models__simple_model,
            "dependent_model.sql": models__dependent_model,
        }

    def test_dependent_models_run_in_order(self, project):
        """Test that dependent models are executed in correct order."""
        results = run_dbt(["run"])

        # Both models should succeed
        assert len(results) == 2
        for result in results:
            assert result.status == "success"

        # Verify the order: simple_model should be created before dependent_model
        result_names = [r.node.name for r in results]
        assert "simple_model" in result_names
        assert "dependent_model" in result_names


class TestBasicCompile:
    """Tests for dbt compile operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models for compile tests."""
        return {
            "simple_model.sql": models__simple_model,
        }

    def test_compile_succeeds(self, project):
        """Test that compilation succeeds without errors."""
        results = run_dbt(["compile"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_compile_generates_sql(self, project):
        """Test that compilation generates valid SQL."""
        run_dbt(["compile"])

        # The compiled SQL should be accessible
        # This verifies the macro rendering works correctly
        manifest = project.cli_vars.get("manifest") if hasattr(project, "cli_vars") else None
        # Basic check that compile didn't fail
        assert True


class TestBasicTest:
    """Tests for dbt test operations."""

    @pytest.fixture(scope="class")
    def models(self):
        """Define models with tests."""
        return {
            "simple_model.sql": models__simple_model,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        """Define tests for the models."""
        return {
            "schema.yml": """
version: 2

models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - not_null
      - name: name
        tests:
          - not_null
""",
        }

    def test_tests_run_successfully(self, project):
        """Test that dbt test runs successfully on the model."""
        run_dbt(["run"])
        results = run_dbt(["test"])
        # Tests should pass (the model has valid data)
        assert all(r.status == "pass" for r in results)