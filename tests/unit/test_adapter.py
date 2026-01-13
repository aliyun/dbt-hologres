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
