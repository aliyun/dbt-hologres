"""Unit test fixtures for dbt-hologres."""
import pytest
from unittest import mock


# Mark all tests in this directory as unit tests
pytestmark = pytest.mark.unit


@pytest.fixture
def mock_psycopg_connect():
    """Mock psycopg connect function."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_handle = mock.MagicMock()
        mock_connect.return_value = mock_handle
        yield mock_connect, mock_handle
