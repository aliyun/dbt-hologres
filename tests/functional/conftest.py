"""Functional test configuration for dbt-hologres.

This module provides the pytest configuration and fixtures required for
functional tests that use the dbt-tests-adapter framework.

Environment Variables:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS: Set to 'true' to run functional tests
    DBT_HOLOGRES_HOST: Hologres instance host
    DBT_HOLOGRES_PORT: Hologres instance port (default: 80)
    DBT_HOLOGRES_USER: Database user
    DBT_HOLOGRES_PASSWORD: Database password
    DBT_HOLOGRES_DATABASE: Database name
    DBT_HOLOGRES_SCHEMA: Schema name (default: public)
    DBT_HOLOGRES_THREADS: Number of threads (default: 4)
"""
import os
import pytest

# Import dbt's test fixtures
# This enables the use of dbt's project fixtures for functional testing
pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target():
    """Provide dbt profile configuration for Hologres tests.

    This fixture returns the target configuration that will be used
    by dbt to connect to the Hologres instance for testing.

    Returns:
        dict: Profile target configuration for Hologres adapter
    """
    return {
        "type": "hologres",
        "threads": int(os.getenv("DBT_HOLOGRES_THREADS", "4")),
        "host": os.getenv("DBT_HOLOGRES_HOST", "localhost"),
        "port": int(os.getenv("DBT_HOLOGRES_PORT", "80")),
        "user": os.getenv("DBT_HOLOGRES_USER", "test_user"),
        "password": os.getenv("DBT_HOLOGRES_PASSWORD", "password"),
        "dbname": os.getenv("DBT_HOLOGRES_DATABASE", "test_db"),
        "schema": os.getenv("DBT_HOLOGRES_SCHEMA", "public"),
    }


@pytest.fixture(scope="class")
def project_config_update():
    """Provide dbt project configuration updates.

    Override this fixture in test classes to add project-specific
    configuration like models, seeds, or macros.

    Returns:
        dict: Project configuration updates
    """
    return {}


def pytest_collection_modifyitems(config, items):
    """Skip functional tests if Hologres connection is not configured.

    This hook ensures that functional tests are only run when
    the environment variable DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS is set to 'true'.
    This prevents test failures in CI/CD environments where a Hologres
    instance may not be available.

    Args:
        config: pytest config object
        items: list of test items to be executed
    """
    skip_marker = pytest.mark.skip(
        reason="Set DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true to run functional tests."
    )

    need_skip = os.getenv("DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS", "").lower() != "true"

    for item in items:
        if "functional" in str(item.fspath) and need_skip:
            item.add_marker(skip_marker)