"""Integration test configuration for dbt-hologres.

This module provides fixtures and configuration for integration tests
that require actual Hologres database connections.

Environment Variables:
    DBT_HOLOGRES_HOST: Hologres instance host
    DBT_HOLOGRES_PORT: Hologres instance port (default: 80)
    DBT_HOLOGRES_USER: Database user
    DBT_HOLOGRES_PASSWORD: Database password
    DBT_HOLOGRES_DATABASE: Database name (default: test_db)
    DBT_HOLOGRES_SCHEMA: Schema name (default: test_schema)
    DBT_HOLOGRES_RUN_INTEGRATION_TESTS: Set to 'true' to run integration tests
"""
import os
import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Generator, Dict, Any
import uuid

from dbt.cli.main import dbtRunner


def skip_if_no_integration_db() -> None:
    """Skip test if integration test environment is not configured."""
    if not os.getenv("DBT_HOLOGRES_RUN_INTEGRATION_TESTS"):
        pytest.skip(
            "Integration tests disabled. Set DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true "
            "to run integration tests."
        )

    required_vars = [
        "DBT_HOLOGRES_HOST",
        "DBT_HOLOGRES_USER",
        "DBT_HOLOGRES_PASSWORD",
        "DBT_HOLOGRES_DATABASE",
    ]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        pytest.skip(f"Missing required environment variables: {', '.join(missing)}")


@pytest.fixture(scope="session")
def integration_credentials() -> Dict[str, Any]:
    """Get Hologres credentials from environment variables."""
    skip_if_no_integration_db()
    return {
        "host": os.getenv("DBT_HOLOGRES_HOST", "localhost"),
        "port": int(os.getenv("DBT_HOLOGRES_PORT", "80")),
        "user": os.getenv("DBT_HOLOGRES_USER", "test_user"),
        "password": os.getenv("DBT_HOLOGRES_PASSWORD", "test_password"),
        "database": os.getenv("DBT_HOLOGRES_DATABASE", "test_db"),
        "schema": os.getenv("DBT_HOLOGRES_SCHEMA", "test_schema"),
    }


@pytest.fixture(scope="session")
def integration_database(integration_credentials: Dict[str, Any]) -> str:
    """Get the database name for integration tests."""
    return integration_credentials["database"]


@pytest.fixture(scope="function")
def unique_schema_name() -> str:
    """Generate a unique schema name for each test to ensure isolation."""
    unique_id = str(uuid.uuid4()).replace("-", "_")[:20]
    return f"test_{unique_id}"


@pytest.fixture(scope="function")
def dbt_project_dir(
    tmp_path: Path,
    integration_credentials: Dict[str, Any],
    unique_schema_name: str,
) -> Generator[Path, None, None]:
    """
    Create a temporary dbt project directory for testing.

    Each test gets its own isolated project directory with:
    - dbt_project.yml
    - profiles.yml
    - models/ directory
    - data/ directory (for seeds)

    The project is automatically cleaned up after the test.
    """
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    dbt_project_config = {
        "name": "test_hologres_integration",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test_hologres",
        "model-paths": ["models"],
        "seed-paths": ["seeds"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
    }

    with open(project_dir / "dbt_project.yml", "w") as f:
        import yaml
        yaml.dump(dbt_project_config, f)

    # Create models directory structure
    (project_dir / "models").mkdir()
    (project_dir / "models" / "staging").mkdir()
    (project_dir / "seeds").mkdir()

    # Create profiles.yml in ~/.dbt/
    dbt_dir = Path.home() / ".dbt"
    dbt_dir.mkdir(exist_ok=True)

    profiles_config = {
        "test_hologres": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "hologres",
                    "host": integration_credentials["host"],
                    "port": integration_credentials["port"],
                    "user": integration_credentials["user"],
                    "pass": integration_credentials["password"],
                    "dbname": integration_credentials["database"],
                    "schema": unique_schema_name,
                    "threads": 1,
                }
            },
        }
    }

    profiles_path = dbt_dir / "profiles.yml"
    # Backup existing profiles if any
    backup_path = None
    if profiles_path.exists():
        backup_path = dbt_dir / "profiles.yml.backup"
        shutil.copy(profiles_path, backup_path)

    try:
        with open(profiles_path, "w") as f:
            import yaml
            yaml.dump(profiles_config, f)

        yield project_dir
    finally:
        # Restore backup if it existed
        if backup_path and backup_path.exists():
            shutil.copy(backup_path, profiles_path)
        elif profiles_path.exists():
            # Only remove if we created it and there's no backup
            try:
                profiles_path.unlink()
            except Exception:
                pass


@pytest.fixture(scope="function")
def dbt_runner(dbt_project_dir: Path) -> dbtRunner:
    """
    Create a dbtRunner instance configured for the test project.

    The runner is configured to use the temporary project directory
    created by the dbt_project_dir fixture.
    """
    os.chdir(dbt_project_dir)
    runner = dbtRunner()
    return runner


@pytest.fixture(scope="function")
def hologres_adapter(
    dbt_runner: dbtRunner,
    integration_credentials: Dict[str, Any],
    unique_schema_name: str,
) -> Generator[Any, None, None]:
    """
    Create a HologresAdapter instance for direct database operations.

    This fixture provides access to the underlying adapter for operations
    that need to bypass dbt's normal execution flow.

    Note: This fixture is currently not implemented.
    Most integration tests should use the dbt_runner fixture instead.
    """
    pytest.skip("Direct adapter instantiation not yet implemented in fixtures")
    yield None


@pytest.fixture(scope="function")
def cleanup_schema(
    dbt_runner: dbtRunner,
    unique_schema_name: str,
    integration_database: str,
) -> Generator[None, None, None]:
    """
    Fixture to ensure schema cleanup after test.

    This fixture attempts to drop the test schema after the test completes,
    even if the test fails. It runs as a finalizer.
    """
    yield

    # Cleanup: drop the schema
    try:
        # Use dbt to drop all models in the schema
        dbt_runner.invoke(["run-operation", "drop_schema", "--args", f"schema_name: {unique_schema_name}"])
    except Exception as e:
        # If dbt cleanup fails, try direct SQL
        try:
            import psycopg
            conn = psycopg.connect(
                host=os.getenv("DBT_HOLOGRES_HOST"),
                port=int(os.getenv("DBT_HOLOGRES_PORT", "80")),
                user=os.getenv("DBT_HOLOGRES_USER"),
                password=os.getenv("DBT_HOLOGRES_PASSWORD"),
                dbname=integration_database,
            )
            cur = conn.cursor()
            cur.execute(f"DROP SCHEMA IF EXISTS {unique_schema_name} CASCADE")
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            # Log but don't fail the test if cleanup fails
            pass


# Helper functions for tests
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
