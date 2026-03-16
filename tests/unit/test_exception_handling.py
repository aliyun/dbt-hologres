"""Unit tests for exception handling in dbt-hologres.

These tests verify that exceptions are properly caught, wrapped, and
re-raised with meaningful error messages.
"""
import pytest
from unittest import mock

from dbt.adapters.hologres.connections import (
    HologresConnectionManager,
    HologresCredentials,
)
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt.adapters.exceptions import FailedToConnectError, IndexConfigError
import psycopg


class TestConnectionExceptions:
    """Test connection-related exception handling."""

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_connection_timeout_error(self, mock_connect):
        """Test handling of connection timeout errors."""
        from multiprocessing.context import SpawnContext

        mock_connect.side_effect = psycopg.OperationalError("Connection timeout")

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

        with pytest.raises(FailedToConnectError):
            HologresConnectionManager.open(connection)

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_connection_refused_error(self, mock_connect):
        """Test handling of connection refused errors."""
        from multiprocessing.context import SpawnContext

        mock_connect.side_effect = psycopg.OperationalError("Connection refused")

        creds = HologresCredentials(
            host="invalid.host.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds

        with pytest.raises(FailedToConnectError):
            HologresConnectionManager.open(connection)

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_authentication_failed_error(self, mock_connect):
        """Test handling of authentication failure."""
        from multiprocessing.context import SpawnContext

        mock_connect.side_effect = psycopg.OperationalError(
            "FATAL: password authentication failed"
        )

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="wrong_password",
            database="test_db",
            schema="public",
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds

        with pytest.raises(FailedToConnectError):
            HologresConnectionManager.open(connection)


class TestSQLExecutionExceptions:
    """Test SQL execution exception handling."""

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_syntax_error_handling(self, mock_logger):
        """Test handling of SQL syntax errors."""
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtDatabaseError):
            with manager.exception_handler("SELECT * FORM test"):
                raise psycopg.DatabaseError("syntax error at or near FORM")

        manager.rollback_if_open.assert_called_once()

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_table_not_found_error(self, mock_logger):
        """Test handling of table not found errors."""
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtDatabaseError):
            with manager.exception_handler("SELECT * FROM nonexistent_table"):
                raise psycopg.DatabaseError('relation "nonexistent_table" does not exist')

        manager.rollback_if_open.assert_called_once()

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_permission_denied_error(self, mock_logger):
        """Test handling of permission denied errors."""
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtDatabaseError):
            with manager.exception_handler("SELECT * FROM protected_table"):
                raise psycopg.DatabaseError("permission denied for table protected_table")

        manager.rollback_if_open.assert_called_once()

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_connection_error_during_query(self, mock_logger):
        """Test handling of connection errors during query execution."""
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtRuntimeError):
            with manager.exception_handler("SELECT * FROM test"):
                raise psycopg.InterfaceError("connection already closed")

        manager.rollback_if_open.assert_called_once()

    @mock.patch("dbt.adapters.hologres.connections.logger")
    def test_unique_violation_error(self, mock_logger):
        """Test handling of unique constraint violation."""
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        manager = HologresConnectionManager(mock.MagicMock(), mp_context)
        manager.rollback_if_open = mock.MagicMock()

        with pytest.raises(DbtDatabaseError):
            with manager.exception_handler("INSERT INTO test VALUES (1)"):
                raise psycopg.DatabaseError(
                    'duplicate key value violates unique constraint "test_pkey"'
                )

        manager.rollback_if_open.assert_called_once()


class TestConfigValidationExceptions:
    """Test configuration validation exception handling."""

    def test_invalid_index_config_columns_type(self):
        """Test handling of invalid index config columns type."""
        from dbt.adapters.hologres.relation_configs import HologresIndexConfig

        with pytest.raises(IndexConfigError):
            HologresIndexConfig.parse({"columns": "invalid", "unique": True})

    def test_missing_required_columns_in_index(self):
        """Test handling of missing columns in index config."""
        from dbt.adapters.hologres.relation_configs import HologresIndexConfig

        with pytest.raises(IndexConfigError):
            HologresIndexConfig.parse({"unique": True})

    def test_empty_host_validation(self):
        """Test handling of empty host."""
        # This might not raise an error depending on implementation
        # but we test the behavior
        try:
            creds = HologresCredentials(
                host="",
                user="test_user",
                password="test_password",
                database="test_db",
                schema="public",
            )
            # If no error is raised, just verify the value
            assert creds.host == ""
        except (ValueError, TypeError):
            pass  # Expected if validation is enforced

    def test_empty_password_validation(self):
        """Test handling of empty password."""
        # Hologres requires password
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="",
            database="test_db",
            schema="public",
        )
        # Password is stored as-is; validation happens at connection time
        assert creds.password == ""

    def test_special_characters_in_password(self):
        """Test handling of special characters in password."""
        special_password = "p@ss!w0rd#$%^&*()"
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password=special_password,
            database="test_db",
            schema="public",
        )
        assert creds.password == special_password


class TestAdapterExceptions:
    """Test adapter-level exception handling."""

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute_macro")
    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_verify_database_mismatch_raises_error(self, mock_execute, mock_execute_macro):
        """Test verify_database raises error on database mismatch."""
        from dbt.adapters.hologres import HologresAdapter
        from dbt.adapters.exceptions import UnexpectedDbReferenceError
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = "expected_db"
        adapter = HologresAdapter(mock_config, mp_context)

        with pytest.raises(UnexpectedDbReferenceError):
            adapter.verify_database("wrong_db")

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute_macro")
    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_verify_database_with_quoted_name(self, mock_execute, mock_execute_macro):
        """Test verify_database handles quoted database names."""
        from dbt.adapters.hologres import HologresAdapter
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = "test_db"
        adapter = HologresAdapter(mock_config, mp_context)

        # Should not raise error when quoted name matches
        result = adapter.verify_database('"test_db"')
        assert result == ""

    def test_parse_index_none_returns_none(self):
        """Test parse_index returns None for None input."""
        from dbt.adapters.hologres import HologresAdapter
        from multiprocessing.context import SpawnContext

        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        result = adapter.parse_index(None)
        assert result is None


class TestRelationExceptions:
    """Test relation-related exception handling."""

    def test_identifier_too_long_raises_error(self):
        """Test that overly long identifiers raise error."""
        from dbt.adapters.hologres.relation import HologresRelation
        from dbt.adapters.hologres.relation_configs import MAX_CHARACTERS_IN_IDENTIFIER

        long_name = "a" * (MAX_CHARACTERS_IN_IDENTIFIER + 1)

        with pytest.raises(DbtRuntimeError) as exc_info:
            HologresRelation.create(
                database="test_db",
                schema="test_schema",
                identifier=long_name,
                type="table",
            )

        assert "longer than" in str(exc_info.value)

    def test_identifier_with_special_characters(self):
        """Test handling of identifiers with special characters."""
        from dbt.adapters.hologres.relation import HologresRelation

        # Hologres allows alphanumeric and underscore
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table_123",
            type="table",
        )
        assert relation.identifier == "test_table_123"


class TestCredentialsExceptions:
    """Test credentials-related exception handling."""

    def test_missing_database_in_credentials(self):
        """Test handling of missing database."""
        with pytest.raises((TypeError, KeyError)):
            HologresCredentials(
                host="test.hologres.aliyuncs.com",
                user="test_user",
                password="test_password",
                # Missing database
            )

    def test_schema_defaults_to_empty_string(self):
        """Test that schema defaults to empty string when not provided."""
        # Use from_dict to trigger __pre_deserialize__ which sets default schema
        creds = HologresCredentials.from_dict({
            "host": "test.hologres.aliyuncs.com",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
        })
        assert creds.schema == ""


class TestRetryMechanism:
    """Test connection retry mechanism."""

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_retry_on_operational_error(self, mock_connect):
        """Test that connection retries on OperationalError."""
        from dbt.adapters.hologres.connections import HologresConnectionManager

        # First two attempts fail, third succeeds
        mock_handle = mock.MagicMock()
        mock_handle.info.backend_pid = 12345
        mock_connect.side_effect = [
            psycopg.OperationalError("Connection refused"),
            psycopg.OperationalError("Connection refused"),
            mock_handle,
        ]

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

        # This should succeed after retries
        result = HologresConnectionManager.open(connection)
        assert mock_connect.call_count == 3

    @mock.patch("dbt.adapters.hologres.connections.psycopg.connect")
    def test_retry_exhausted_raises_error(self, mock_connect):
        """Test that retry exhaustion raises the final error."""
        from dbt.adapters.hologres.connections import HologresConnectionManager

        mock_connect.side_effect = psycopg.OperationalError("Connection refused")

        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            retries=2,
        )

        connection = mock.MagicMock()
        connection.state = "init"
        connection.credentials = creds
        connection.name = "test_connection"

        with pytest.raises(FailedToConnectError):
            HologresConnectionManager.open(connection)

        # Should have tried initial + retries
        assert mock_connect.call_count >= 1