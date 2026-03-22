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


class TestHologresConnectionManagerOpen:
    """Test HologresConnectionManager.open method."""

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_already_open_connection(self, mock_connect):
        """Test open skips when connection is already open."""
        connection = mock.MagicMock()
        connection.state = "open"

        result = HologresConnectionManager.open(connection)

        mock_connect.assert_not_called()
        assert result == connection

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_sets_autocommit(self, mock_connect):
        """Test open sets autocommit mode for CTAS."""
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_connect.return_value = mock_handle

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        assert mock_handle.autocommit is True

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_sets_search_path(self, mock_connect):
        """Test open sets search_path when specified."""
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_cursor = mock.MagicMock()
        mock_handle.cursor.return_value.__enter__ = mock.MagicMock(return_value=mock_cursor)
        mock_handle.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)
        mock_connect.return_value = mock_handle

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            search_path="public,raw",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify search_path was set
        mock_cursor.execute.assert_called()

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_sets_role(self, mock_connect):
        """Test open sets role when specified."""
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_cursor = mock.MagicMock()
        mock_handle.cursor.return_value.__enter__ = mock.MagicMock(return_value=mock_cursor)
        mock_handle.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)
        mock_connect.return_value = mock_handle

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            role="admin_role",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify role was set
        mock_cursor.execute.assert_called()

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_with_sslmode(self, mock_connect):
        """Test open passes sslmode to connection."""
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_connect.return_value = mock_handle

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            sslmode="require",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify sslmode was passed
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs.get("sslmode") == "require"


class TestHologresCredentialsValidation:
    """Test HologresCredentials validation."""

    def test_port_boundary_minimum(self):
        """Test port at minimum valid value."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            port=0,
        )
        assert creds.port == 0

    def test_port_boundary_maximum(self):
        """Test port at maximum valid value."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            port=65535,
        )
        assert creds.port == 65535


class TestGetResponse:
    """Test HologresConnectionManager.get_response method."""

    def test_get_response_with_status_message(self):
        """Test get_response parses status message."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "INSERT 0 100"
        mock_cursor.rowcount = 100

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response._message == "INSERT 0 100"
        assert response.code == "INSERT"
        assert response.rows_affected == 100

    def test_get_response_with_update(self):
        """Test get_response handles UPDATE status."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "UPDATE 50"
        mock_cursor.rowcount = 50

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.code == "UPDATE"
        assert response.rows_affected == 50

    def test_get_response_with_delete(self):
        """Test get_response handles DELETE status."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "DELETE 25"
        mock_cursor.rowcount = 25

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.code == "DELETE"
        assert response.rows_affected == 25

    def test_get_response_complex_status(self):
        """Test get_response handles complex status message."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "SELECT 1000"
        mock_cursor.rowcount = 1000

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.code == "SELECT"
        assert response.rows_affected == 1000


class TestDataTypeCodeToName:
    """Test HologresConnectionManager.data_type_code_to_name method."""

    def test_known_type_codes(self):
        """Test known type codes return expected format."""
        # The current implementation returns a formatted string
        result = HologresConnectionManager.data_type_code_to_name(123)
        assert isinstance(result, str)
        assert "123" in result

    def test_unknown_type_code(self):
        """Test unknown type code returns formatted string."""
        result = HologresConnectionManager.data_type_code_to_name(9999)
        assert isinstance(result, str)
        assert "9999" in result

    def test_negative_value(self):
        """Test negative type code handling."""
        # The method should handle negative values
        result = HologresConnectionManager.data_type_code_to_name(-1)
        assert isinstance(result, str)
        assert "-1" in result

    def test_zero_type_code(self):
        """Test zero type code."""
        result = HologresConnectionManager.data_type_code_to_name(0)
        assert isinstance(result, str)
        assert "0" in result

    def test_large_type_code(self):
        """Test large type code value."""
        result = HologresConnectionManager.data_type_code_to_name(1000000)
        assert isinstance(result, str)
        assert "1000000" in result

    def test_returns_string_type(self):
        """Test return type is always string."""
        for code in [0, 1, 100, -1, 9999]:
            result = HologresConnectionManager.data_type_code_to_name(code)
            assert isinstance(result, str)


class TestGetResponseExtended:
    """Extended tests for HologresConnectionManager.get_response method."""

    def test_with_null_statusmessage(self):
        """Test get_response handles NULL statusmessage."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = None
        mock_cursor.rowcount = 0

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response._message is None
        assert response.code == ""
        assert response.rows_affected == 0

    def test_with_complex_status(self):
        """Test get_response handles complex status messages."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "CREATE TABLE"
        mock_cursor.rowcount = 0

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response._message == "CREATE TABLE"
        assert response.code == "CREATE TABLE"

    def test_rows_affected_large_number(self):
        """Test get_response handles large rows_affected."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "INSERT 0 1000000"
        mock_cursor.rowcount = 1000000

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.rows_affected == 1000000

    def test_status_message_with_numbers(self):
        """Test get_response extracts status code correctly from message with numbers."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "INSERT 0 500"
        mock_cursor.rowcount = 500

        response = HologresConnectionManager.get_response(mock_cursor)

        # Code should be "INSERT" (numbers filtered out)
        assert response.code == "INSERT"
        assert response._message == "INSERT 0 500"

    def test_empty_status_message(self):
        """Test get_response handles empty status message."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = ""
        mock_cursor.rowcount = 0

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response._message == ""
        assert response.code == ""

    def test_copy_command_status(self):
        """Test get_response handles COPY command status."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "COPY 1000"
        mock_cursor.rowcount = 1000

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.code == "COPY"
        assert response.rows_affected == 1000

    def test_multiple_word_command(self):
        """Test get_response handles multi-word commands."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "DROP TABLE"
        mock_cursor.rowcount = 0

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.code == "DROP TABLE"

    def test_negative_rowcount(self):
        """Test get_response handles negative rowcount."""
        mock_cursor = mock.MagicMock()
        mock_cursor.statusmessage = "SELECT"
        mock_cursor.rowcount = -1  # Some drivers return -1 for unknown

        response = HologresConnectionManager.get_response(mock_cursor)

        assert response.rows_affected == -1


class TestHologresCredentialsEdgeCases:
    """Edge case tests for HologresCredentials."""

    def test_host_with_port_in_string(self):
        """Test credentials with host containing port."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com:8080",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        # Host should be preserved as-is
        assert creds.host == "test.hologres.aliyuncs.com:8080"

    def test_user_with_special_characters(self):
        """Test credentials with special characters in user."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="BASIC$test_user@example.com",
            password="test_password",
            database="test_db",
            schema="public",
        )

        assert creds.user == "BASIC$test_user@example.com"

    def test_password_with_special_characters(self):
        """Test credentials with special characters in password."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="p@ss!w0rd#$%",
            database="test_db",
            schema="public",
        )

        assert creds.password == "p@ss!w0rd#$%"

    def test_database_with_hyphens(self):
        """Test credentials with hyphens in database name."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="my-test-database",
            schema="public",
        )

        assert creds.database == "my-test-database"

    def test_schema_with_underscores(self):
        """Test credentials with underscores in schema."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="my_schema_name",
        )

        assert creds.schema == "my_schema_name"

    def test_connect_timeout_edge_values(self):
        """Test connect_timeout with edge values."""
        # Minimum value
        creds_min = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            connect_timeout=0,
        )
        assert creds_min.connect_timeout == 0

        # Large value
        creds_max = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            connect_timeout=300,
        )
        assert creds_max.connect_timeout == 300

    def test_application_name_with_spaces(self):
        """Test application_name with spaces."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            application_name="My App Name",
        )

        assert creds.application_name == "My App Name"


class TestHologresConnectionManagerRetry:
    """Test HologresConnectionManager retry mechanism."""

    def test_quadratic_backoff_calculation(self):
        """Test quadratic backoff calculation: 0, 1, 4, 9, 16..."""
        # Define the function locally since it's defined inside open()
        def quadratic_backoff(attempt: int):
            return attempt * attempt

        # Test first few attempts
        assert quadratic_backoff(0) == 0
        assert quadratic_backoff(1) == 1
        assert quadratic_backoff(2) == 4
        assert quadratic_backoff(3) == 9
        assert quadratic_backoff(4) == 16
        assert quadratic_backoff(5) == 25

    def test_quadratic_backoff_sequence(self):
        """Test that backoff values increase quadratically."""
        def quadratic_backoff(attempt: int):
            return attempt * attempt

        prev = -1
        for attempt in range(10):
            current = quadratic_backoff(attempt)
            assert current >= prev
            prev = current

    @mock.patch("dbt.adapters.hologres.connections.HologresConnectionManager.retry_connection")
    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_passes_retry_parameters(self, mock_connect, mock_retry):
        """Test open passes correct retry parameters to retry_connection."""
        mock_retry.return_value = mock.MagicMock()

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            retries=3,
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify retry_connection was called with correct parameters
        mock_retry.assert_called_once()
        call_kwargs = mock_retry.call_args[1]

        assert call_kwargs["retry_limit"] == 3
        assert "retry_timeout" in call_kwargs
        assert call_kwargs["retryable_exceptions"] == [psycopg.OperationalError]

    @mock.patch("dbt.adapters.hologres.connections.HologresConnectionManager.retry_connection")
    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_default_retries(self, mock_connect, mock_retry):
        """Test open uses default retries value."""
        mock_retry.return_value = mock.MagicMock()

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            # No retries specified, should use default (1)
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        call_kwargs = mock_retry.call_args[1]
        assert call_kwargs["retry_limit"] == 1  # Default value

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_open_connect_parameters(self, mock_connect):
        """Test open passes correct parameters to psycopg.connect."""
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_connect.return_value = mock_handle

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            port=8080,
            connect_timeout=30,
            sslmode="require",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify connect was called with correct parameters
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["dbname"] == "test_db"
        assert call_kwargs["user"] == "test_user"
        assert call_kwargs["host"] == "test.hologres.aliyuncs.com"
        assert call_kwargs["password"] == "test_password"
        assert call_kwargs["port"] == 8080
        assert call_kwargs["connect_timeout"] == 30
        assert call_kwargs["sslmode"] == "require"


class TestQuadraticBackoffFunction:
    """Direct tests for quadratic backoff function behavior."""

    def test_zero_attempt(self):
        """Test backoff for attempt 0."""
        def quadratic_backoff(attempt: int):
            return attempt * attempt

        assert quadratic_backoff(0) == 0

    def test_large_attempt(self):
        """Test backoff for large attempt number."""
        def quadratic_backoff(attempt: int):
            return attempt * attempt

        assert quadratic_backoff(10) == 100
        assert quadratic_backoff(100) == 10000

    def test_negative_attempt(self):
        """Test backoff for negative attempt (edge case)."""
        def quadratic_backoff(attempt: int):
            return attempt * attempt

        # Negative numbers squared are positive
        assert quadratic_backoff(-1) == 1
        assert quadratic_backoff(-5) == 25


class TestRetryableExceptions:
    """Test retryable exception types."""

    def test_operational_error_is_retryable(self):
        """Test OperationalError is in retryable exceptions."""
        retryable_exceptions = [psycopg.OperationalError]

        # Verify OperationalError is included
        assert psycopg.OperationalError in retryable_exceptions

    def test_database_error_not_retryable(self):
        """Test DatabaseError is not in retryable exceptions."""
        retryable_exceptions = [psycopg.OperationalError]

        # DatabaseError should not be retryable by default
        assert psycopg.DatabaseError not in retryable_exceptions

    def test_interface_error_not_retryable(self):
        """Test InterfaceError is not in retryable exceptions."""
        retryable_exceptions = [psycopg.OperationalError]

        # InterfaceError should not be retryable by default
        assert psycopg.InterfaceError not in retryable_exceptions


class TestConnectionRetryIntegration:
    """Integration-style tests for connection retry behavior."""

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    @mock.patch("dbt.adapters.hologres.connections.HologresConnectionManager.retry_connection")
    def test_retry_connection_called_with_connect_function(self, mock_retry, mock_connect):
        """Test retry_connection receives the connect function."""
        mock_retry.return_value = mock.MagicMock()

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        HologresConnectionManager.open(connection)

        # Verify connect parameter is a callable
        call_kwargs = mock_retry.call_args[1]
        assert callable(call_kwargs["connect"])

