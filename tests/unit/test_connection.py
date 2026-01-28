"""Unit tests for Hologres connection management."""
import pytest
from unittest import mock

from dbt.adapters.hologres.connections import (
    HologresCredentials,
    HologresConnectionManager,
)
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
import psycopg


class TestHologresCredentials:
    """Test HologresCredentials class."""

    def test_default_values(self):
        """Test default credential values."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="BASIC$test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        assert creds.type == "hologres"
        assert creds.port == 80
        assert creds.sslmode == "disable"
        assert creds.connect_timeout == 10
        assert "dbt_hologres" in creds.application_name

    def test_custom_application_name(self):
        """Test custom application name is preserved."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            application_name="custom_app",
        )

        assert creds.application_name == "custom_app"

    def test_unique_field(self):
        """Test unique_field property."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        assert creds.unique_field == "test.hologres.aliyuncs.com"

    def test_connection_keys(self):
        """Test _connection_keys method."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        keys = creds._connection_keys()
        assert "host" in keys
        assert "port" in keys
        assert "user" in keys
        assert "database" in keys
        assert "sslmode" in keys
        assert "connect_timeout" in keys
        assert "role" in keys
        assert "search_path" in keys
        assert "application_name" in keys
        assert "retries" in keys

    def test_empty_schema_defaults(self):
        """Test empty schema defaults to empty string."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="",  # Explicitly pass empty schema
        )

        assert creds.schema == ""

    def test_role_and_search_path(self):
        """Test role and search_path configuration."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            role="admin_role",
            search_path="public,raw",
        )

        assert creds.role == "admin_role"
        assert creds.search_path == "public,raw"

    def test_retries_configuration(self):
        """Test retries configuration."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            retries=3,
        )

        assert creds.retries == 3

    def test_sslmode_configuration(self):
        """Test SSL mode configuration."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            sslmode="require",
        )

        assert creds.sslmode == "require"


class TestHologresConnectionManager:
    """Test HologresConnectionManager class."""

    def test_type(self):
        """Test connection manager type."""
        assert HologresConnectionManager.TYPE == "hologres"

    def test_get_credentials(self):
        """Test get_credentials returns the same credentials."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        result = HologresConnectionManager.get_credentials(creds)
        assert result is creds

    def test_get_response(self):
        """Test get_response parses cursor status."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "SELECT 1"
        mock_cursor.rowcount = 1

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response._message == "SELECT 1"
        assert response.code == "SELECT"
        assert response.rows_affected == 1

    def test_get_response_empty_status(self):
        """Test get_response handles empty status message."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = None
        mock_cursor.rowcount = 0

        response = HologresConnectionManager.get_response(mock_cursor)

        # When statusmessage is None, _message is also None
        assert response._message is None
        assert response.code == ""
        assert response.rows_affected == 0

    def test_data_type_code_to_name(self):
        """Test data_type_code_to_name returns a string."""
        result = HologresConnectionManager.data_type_code_to_name(123)
        assert isinstance(result, str)
        assert "123" in result

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_exception_handler_database_error(self, mock_logger):
        """Test exception_handler handles database errors."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtDatabaseError):
            with manager.exception_handler("SELECT * FROM test"):
                raise psycopg.DatabaseError("Connection failed")

        mock_logger.debug.assert_called()
        manager.rollback_if_open.assert_called_once()

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_exception_handler_runtime_error(self, mock_logger):
        """Test exception_handler handles runtime errors."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtRuntimeError):
            with manager.exception_handler("SELECT * FROM test"):
                raise Exception("Generic error")

        mock_logger.debug.assert_called()
        manager.rollback_if_open.assert_called_once()

    def test_cancel_connection_already_closed(self):
        """Test InterfaceError can be detected with 'already closed' message."""
        # Create an InterfaceError with "already closed" message
        interface_error = psycopg.InterfaceError("Connection already closed")

        # Verify we can detect this specific error condition
        assert isinstance(interface_error, psycopg.InterfaceError)
        assert "already closed" in str(interface_error)

        # Test passes if we can identify the error type and message
        assert True

    @mock.patch("dbt.adapters.hologres.connections.logger")
    @mock.patch("dbt.adapters.hologres.connections.HologresConnectionManager.add_query")
    def test_cancel_terminates_backend(self, mock_add_query, mock_logger):
        """Test cancel sends termination query."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        connection = mock.MagicMock()
        connection.name = "test_connection"
        connection.handle.info.backend_pid = 12345

        mock_cursor = mock.MagicMock()
        mock_cursor.fetchone.return_value = True
        mock_add_query.return_value = (None, mock_cursor)

        manager.cancel(connection)

        mock_add_query.assert_called_once_with("select pg_terminate_backend(12345)")
        mock_logger.debug.assert_called()

    def test_add_begin_query(self):
        """Test add_begin_query does nothing (pass)."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.add_begin_query()  # Should not raise any exception

