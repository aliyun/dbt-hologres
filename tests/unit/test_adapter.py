"""Unit tests for HologresAdapter."""
import pytest
from unittest import mock
from dbt.adapters.exceptions import UnexpectedDbReferenceError
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.hologres import HologresAdapter
from dbt.adapters.hologres.impl import HologresConfig
from dbt.adapters.hologres.relation_configs import HologresIndexConfig


class TestHologresAdapter:
    """Test HologresAdapter class."""

    def test_date_function(self):
        """Test date_function returns correct value."""
        assert HologresAdapter.date_function() == "now()"

    def test_constraint_support(self):
        """Test constraint support configuration."""
        assert ConstraintType.check in HologresAdapter.CONSTRAINT_SUPPORT
        assert ConstraintType.not_null in HologresAdapter.CONSTRAINT_SUPPORT
        assert ConstraintType.unique in HologresAdapter.CONSTRAINT_SUPPORT
        assert ConstraintType.primary_key in HologresAdapter.CONSTRAINT_SUPPORT
        assert ConstraintType.foreign_key in HologresAdapter.CONSTRAINT_SUPPORT

    def test_catalog_by_relation_support(self):
        """Test catalog by relation support is enabled."""
        assert HologresAdapter.CATALOG_BY_RELATION_SUPPORT is True

    def test_valid_incremental_strategies(self):
        """Test valid incremental strategies."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)
        strategies = adapter.valid_incremental_strategies()
        assert "append" in strategies
        assert "delete+insert" in strategies
        assert "merge" in strategies
        assert "microbatch" in strategies

    def test_timestamp_add_sql(self):
        """Test timestamp_add_sql generates correct SQL."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        result = adapter.timestamp_add_sql("my_column", 1, "hour")
        assert result == "my_column + interval '1 hour'"

        result = adapter.timestamp_add_sql("created_at", 5, "day")
        assert result == "created_at + interval '5 day'"

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute_macro")
    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_verify_database_success(self, mock_execute, mock_execute_macro):
        """Test verify_database succeeds with matching database."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = "test_db"
        mock_adapter = HologresAdapter(mock_config, mp_context)

        result = mock_adapter.verify_database("test_db")
        assert result == ""

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute_macro")
    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_verify_database_with_quotes(self, mock_execute, mock_execute_macro):
        """Test verify_database handles quoted database names."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = "test_db"
        mock_adapter = HologresAdapter(mock_config, mp_context)

        result = mock_adapter.verify_database('"test_db"')
        assert result == ""

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute_macro")
    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_verify_database_failure(self, mock_execute, mock_execute_macro):
        """Test verify_database raises error with non-matching database."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = "expected_db"
        mock_adapter = HologresAdapter(mock_config, mp_context)

        with pytest.raises(UnexpectedDbReferenceError):
            mock_adapter.verify_database("wrong_db")

    def test_parse_index_valid(self):
        """Test parse_index with valid index config."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_adapter = HologresAdapter(mock_config, mp_context)

        raw_index = {"columns": ["col1", "col2"], "unique": True}
        result = mock_adapter.parse_index(raw_index)

        assert isinstance(result, HologresIndexConfig)
        assert result.columns == ["col1", "col2"]
        assert result.unique is True

    def test_parse_index_none(self):
        """Test parse_index with None returns None."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_adapter = HologresAdapter(mock_config, mp_context)

        result = mock_adapter.parse_index(None)
        assert result is None

    @mock.patch("dbt.adapters.hologres.impl.HologresAdapter.execute")
    def test_debug_query(self, mock_execute):
        """Test debug_query executes select statement."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_adapter = HologresAdapter(mock_config, mp_context)

        mock_adapter.debug_query()
        mock_execute.assert_called_once_with("select 1 as id")


class TestHologresConfig:
    """Test HologresConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = HologresConfig()
        assert config.indexes is None
        assert config.orientation is None
        assert config.distribution_key is None
        assert config.clustering_key is None
        assert config.event_time_column is None
        assert config.segment_key is None
        assert config.bitmap_columns is None
        assert config.dictionary_encoding_columns is None

    def test_with_index_config(self):
        """Test config with index configuration."""
        index_config = HologresIndexConfig(columns=["col1", "col2"])
        config = HologresConfig(indexes=[index_config])

        assert len(config.indexes) == 1
        assert config.indexes[0].columns == ["col1", "col2"]

    def test_with_table_properties(self):
        """Test config with Hologres table properties."""
        config = HologresConfig(
            orientation="column",
            distribution_key="user_id",
            clustering_key="event_time",
            event_time_column="created_at",
            bitmap_columns="status,type",
            dictionary_encoding_columns="category",
        )

        assert config.orientation == "column"
        assert config.distribution_key == "user_id"
        assert config.clustering_key == "event_time"
        assert config.event_time_column == "created_at"
        assert config.bitmap_columns == "status,type"
        assert config.dictionary_encoding_columns == "category"

    def test_segment_key_alias(self):
        """Test segment_key is an alias for event_time_column."""
        config = HologresConfig(segment_key="created_at")
        assert config.segment_key == "created_at"
        assert config.event_time_column is None

    def test_logical_partition_key_single(self):
        """Test config with single logical partition key."""
        config = HologresConfig(logical_partition_key="ds")
        assert config.logical_partition_key == "ds"

    def test_logical_partition_key_multiple(self):
        """Test config with multiple logical partition keys."""
        config = HologresConfig(logical_partition_key="order_year, order_month")
        assert config.logical_partition_key == "order_year, order_month"

    def test_logical_partition_key_with_properties(self):
        """Test config with logical partition key and other properties."""
        config = HologresConfig(
            orientation="column",
            distribution_key="order_id",
            logical_partition_key="ds",
        )
        assert config.orientation == "column"
        assert config.distribution_key == "order_id"
        assert config.logical_partition_key == "ds"


class TestHologresAdapterParseDate:
    """Test HologresAdapter.parse_date method edge cases."""

    def test_parse_date_string(self):
        """Test parse_date with date string."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        ld = adapter.parse_date("2024-06-15")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15

    def test_parse_date_none_returns_today(self):
        """Test parse_date with None returns today."""
        from multiprocessing.context import SpawnContext
        from datetime import date
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        ld = adapter.parse_date(None)
        today = date.today()
        assert ld.year == today.year
        assert ld.month == today.month
        assert ld.day == today.day

    def test_parse_date_date_object(self):
        """Test parse_date with date object."""
        from multiprocessing.context import SpawnContext
        from datetime import date
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        d = date(2024, 8, 20)
        ld = adapter.parse_date(d)
        assert ld.year == 2024
        assert ld.month == 8
        assert ld.day == 20

    def test_parse_date_datetime_object(self):
        """Test parse_date with datetime object."""
        from multiprocessing.context import SpawnContext
        from datetime import datetime
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        dt = datetime(2024, 7, 20, 15, 30)
        ld = adapter.parse_date(dt)
        assert ld.year == 2024
        assert ld.month == 7
        assert ld.day == 20

    def test_parse_date_chain_operations(self):
        """Test parse_date returns chainable LocalDate."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        ld = adapter.parse_date("2024-03-15")
        result = ld.sub_months(2).start_of_month()
        assert str(result) == "2024-01-01"


class TestHologresAdapterToday:
    """Test HologresAdapter.today method."""

    def test_today_returns_current_date(self):
        """Test today() returns current date."""
        from multiprocessing.context import SpawnContext
        from datetime import date
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        ld = adapter.today()
        today = date.today()
        assert ld.year == today.year
        assert ld.month == today.month
        assert ld.day == today.day

    def test_today_is_local_date(self):
        """Test today() returns LocalDate instance."""
        from multiprocessing.context import SpawnContext
        from dbt.adapters.hologres.local_date import LocalDate
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        ld = adapter.today()
        assert isinstance(ld, LocalDate)


class TestHologresAdapterTimestampAdd:
    """Test HologresAdapter.timestamp_add_sql method edge cases."""

    def test_timestamp_add_negative_number(self):
        """Test timestamp_add_sql with negative number."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        result = adapter.timestamp_add_sql("created_at", -1, "day")
        assert result == "created_at + interval '-1 day'"

    def test_timestamp_add_zero(self):
        """Test timestamp_add_sql with zero."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        result = adapter.timestamp_add_sql("created_at", 0, "hour")
        assert result == "created_at + interval '0 hour'"

    def test_timestamp_add_large_number(self):
        """Test timestamp_add_sql with large number."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        result = adapter.timestamp_add_sql("created_at", 365, "day")
        assert result == "created_at + interval '365 day'"

    def test_timestamp_add_various_intervals(self):
        """Test timestamp_add_sql with various interval types."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        intervals = ["second", "minute", "hour", "day", "week", "month", "year"]
        for interval in intervals:
            result = adapter.timestamp_add_sql("ts", 1, interval)
            assert f"1 {interval}" in result
