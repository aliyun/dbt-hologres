"""Unit test fixtures for dbt-hologres."""
import pytest
from unittest import mock
from jinja2 import Environment, FileSystemLoader
import os


# Mark all tests in this directory as unit tests
pytestmark = pytest.mark.unit


@pytest.fixture
def mock_psycopg_connect():
    """Mock psycopg connect function."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_handle = mock.MagicMock()
        mock_connect.return_value = mock_handle
        yield mock_connect, mock_handle


@pytest.fixture
def mock_adapter():
    """Mock HologresAdapter instance with common methods."""
    from multiprocessing.context import SpawnContext
    from dbt.adapters.hologres import HologresAdapter

    mp_context = SpawnContext()
    mock_config = mock.MagicMock()
    mock_config.credentials.database = "test_db"
    mock_config.credentials.schema = "public"

    adapter = HologresAdapter(mock_config, mp_context)
    adapter._connection_manager = mock.MagicMock()
    adapter.execute = mock.MagicMock(return_value=([], mock.MagicMock()))
    adapter.execute_macro = mock.MagicMock()

    return adapter


@pytest.fixture
def jinja_environment():
    """Create a Jinja2 environment for testing SQL macros."""
    # Get the path to macros directory
    macro_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "src",
        "dbt",
        "include",
        "hologres",
        "macros"
    )

    env = Environment(
        loader=FileSystemLoader(macro_path),
        extensions=["jinja2.ext.do"],
    )

    # Add dbt-like context functions
    def mock_config(key, default=None):
        """Mock config.get() function."""
        return mock.MagicMock(get=lambda k, d=None: d or default)

    env.globals["config"] = mock.MagicMock(get=lambda k, d=None: d)
    env.globals["var"] = lambda k, d=None: d
    env.globals["env_var"] = lambda k, d=None: d
    env.globals["adapter"] = mock.MagicMock()
    env.globals["database"] = "test_db"
    env.globals["exceptions"] = mock.MagicMock()
    env.globals["exceptions"].raise_compiler_error = lambda msg: (_ for _ in ()).throw(Exception(msg))

    return env


@pytest.fixture
def mock_relation():
    """Mock relation object for testing."""
    relation = mock.MagicMock()
    relation.identifier = "test_table"
    relation.schema = "test_schema"
    relation.database = "test_db"
    relation.type = "table"
    relation.__str__ = lambda self: "test_db.test_schema.test_table"
    relation.include = mock.MagicMock(return_value=relation)
    relation.without_identifier = mock.MagicMock(return_value=relation)

    return relation


@pytest.fixture
def mock_column():
    """Mock column object for testing DDL generation."""
    column = mock.MagicMock()
    column.column = "test_column"
    column.data_type = "TEXT"
    column.name = "test_column"
    column.dtype = "TEXT"

    return column


@pytest.fixture
def sample_columns():
    """Sample column list for testing DDL generation."""
    columns = []
    for name, dtype in [
        ("id", "BIGINT"),
        ("name", "TEXT"),
        ("ds", "TEXT"),
        ("created_at", "TIMESTAMP"),
    ]:
        col = mock.MagicMock()
        col.column = name
        col.data_type = dtype
        columns.append(col)
    return columns


@pytest.fixture
def mock_relation_config():
    """Mock RelationConfig for testing configuration changes."""
    config = mock.MagicMock()
    config.config = mock.MagicMock()
    config.config.extra = {}
    return config


@pytest.fixture
def mock_relation_results():
    """Mock RelationResults for testing configuration changes."""
    results = mock.MagicMock()
    return results
