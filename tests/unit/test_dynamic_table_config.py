"""Unit tests for HologresDynamicTableConfig."""
import pytest
from unittest import mock

from dbt.adapters.hologres.relation_configs import (
    HologresDynamicTableConfig,
    HologresDynamicTableConfigChangeCollection,
)


class TestHologresDynamicTableConfig:
    """Test HologresDynamicTableConfig class."""

    def test_create_minimal_config(self):
        """Test creating minimal dynamic table config with only required fields."""
        config = HologresDynamicTableConfig(freshness="30 minutes")

        assert config.freshness == "30 minutes"
        assert config.auto_refresh_enable is True  # default
        assert config.auto_refresh_mode == "auto"  # default
        assert config.computing_resource == "serverless"  # default
        assert config.base_table_cdc_format == "stream"  # default

    def test_create_full_config(self):
        """Test creating dynamic table config with all fields."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_enable=False,
            auto_refresh_mode="incremental",
            computing_resource="my_warehouse",
            base_table_cdc_format="binlog",
            partition_key="event_date",
            partition_type="physical",
            partition_key_time_format="yyyy-MM-dd",
            auto_refresh_partition_active_time="00:00-23:59",
            orientation="column",
            distribution_key=["user_id"],
            clustering_key=["event_time"],
            event_time_column=["created_at"],
            bitmap_columns=["status", "type"],
            dictionary_encoding_columns=["category"],
            time_to_live_in_seconds=86400,
            storage_mode="hot",
        )

        assert config.freshness == "1 hours"
        assert config.auto_refresh_enable is False
        assert config.auto_refresh_mode == "incremental"
        assert config.computing_resource == "my_warehouse"
        assert config.partition_key == "event_date"
        assert config.orientation == "column"
        assert config.time_to_live_in_seconds == 86400

    def test_from_dict_required_only(self):
        """Test from_dict with only required field."""
        config_dict = {"freshness": "2 hours"}

        config = HologresDynamicTableConfig.from_dict(config_dict)

        assert config.freshness == "2 hours"
        assert config.auto_refresh_enable is True  # default applied
        assert config.auto_refresh_mode == "auto"  # default applied

    def test_from_dict_with_optional_fields(self):
        """Test from_dict with optional fields."""
        config_dict = {
            "freshness": "15 minutes",
            "auto_refresh_enable": False,
            "auto_refresh_mode": "full",
            "computing_resource": "local",
            "orientation": "row",
        }

        config = HologresDynamicTableConfig.from_dict(config_dict)

        assert config.freshness == "15 minutes"
        assert config.auto_refresh_enable is False
        assert config.auto_refresh_mode == "full"
        assert config.computing_resource == "local"
        assert config.orientation == "row"

    def test_from_config(self):
        """Test from_config extracts from RelationConfig."""
        relation_config = mock.MagicMock()
        relation_config.config.extra = {
            "freshness": "1 hours",
            "auto_refresh_mode": "auto",
            "auto_refresh_enable": True,
            "computing_resource": "serverless",
        }

        config = HologresDynamicTableConfig.from_config(relation_config)

        assert config.freshness == "1 hours"
        assert config.auto_refresh_mode == "auto"

    def test_from_config_empty_extra(self):
        """Test from_config with empty extra config raises MissingField error."""
        from mashumaro.exceptions import MissingField

        relation_config = mock.MagicMock()
        relation_config.config.extra = {}

        # Should raise error due to missing required freshness field
        with pytest.raises(MissingField):
            HologresDynamicTableConfig.from_config(relation_config)

    def test_from_relation_results(self):
        """Test from_relation_results creates config."""
        relation_results = mock.MagicMock()

        config = HologresDynamicTableConfig.from_relation_results(relation_results)

        # Returns minimal config with default freshness
        assert config.freshness == "1 hours"

    def test_equality(self):
        """Test config equality."""
        config1 = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_mode="auto",
        )
        config2 = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_mode="auto",
        )

        assert config1 == config2

    def test_inequality(self):
        """Test config inequality."""
        config1 = HologresDynamicTableConfig(freshness="1 hours")
        config2 = HologresDynamicTableConfig(freshness="2 hours")

        assert config1 != config2


class TestHologresDynamicTableConfigChangeCollection:
    """Test HologresDynamicTableConfigChangeCollection class."""

    def test_default_empty_collection(self):
        """Test default collection has no changes."""
        collection = HologresDynamicTableConfigChangeCollection()

        assert collection.freshness is None
        assert collection.auto_refresh_mode is None
        assert collection.auto_refresh_enable is None
        assert collection.computing_resource is None

    def test_has_changes_false(self):
        """Test has_changes returns False when no changes."""
        collection = HologresDynamicTableConfigChangeCollection()

        assert collection.has_changes is False

    def test_has_changes_true_freshness(self):
        """Test has_changes returns True with freshness change."""
        collection = HologresDynamicTableConfigChangeCollection(
            freshness="30 minutes"
        )

        assert collection.has_changes is True

    def test_has_changes_true_auto_refresh_mode(self):
        """Test has_changes returns True with auto_refresh_mode change."""
        collection = HologresDynamicTableConfigChangeCollection(
            auto_refresh_mode="incremental"
        )

        assert collection.has_changes is True

    def test_has_changes_true_auto_refresh_enable(self):
        """Test has_changes returns True with auto_refresh_enable change."""
        collection = HologresDynamicTableConfigChangeCollection(
            auto_refresh_enable=False
        )

        assert collection.has_changes is True

    def test_has_changes_true_computing_resource(self):
        """Test has_changes returns True with computing_resource change."""
        collection = HologresDynamicTableConfigChangeCollection(
            computing_resource="warehouse"
        )

        assert collection.has_changes is True

    def test_has_changes_multiple(self):
        """Test has_changes returns True with multiple changes."""
        collection = HologresDynamicTableConfigChangeCollection(
            freshness="1 hours",
            auto_refresh_mode="full",
        )

        assert collection.has_changes is True

    def test_requires_full_refresh_false(self):
        """Test requires_full_refresh is always False."""
        collection = HologresDynamicTableConfigChangeCollection(
            freshness="1 hours",
            auto_refresh_mode="full",
        )

        assert collection.requires_full_refresh is False


class TestHologresDynamicTableConfigDefaultValues:
    """Test all default values for HologresDynamicTableConfig."""

    def test_all_default_values(self):
        """Test all optional fields have correct default values."""
        config = HologresDynamicTableConfig(freshness="30 minutes")

        # Required field
        assert config.freshness == "30 minutes"

        # Default values for optional fields
        assert config.auto_refresh_enable is True
        assert config.auto_refresh_mode == "auto"
        assert config.computing_resource == "serverless"
        assert config.base_table_cdc_format == "stream"
        assert config.partition_key is None
        assert config.partition_type is None
        assert config.partition_key_time_format is None
        assert config.auto_refresh_partition_active_time is None
        assert config.orientation == "column"
        assert config.distribution_key is None
        assert config.clustering_key is None
        assert config.event_time_column is None
        assert config.bitmap_columns is None
        assert config.dictionary_encoding_columns is None
        assert config.time_to_live_in_seconds is None
        assert config.storage_mode == "hot"

    def test_time_to_live_in_seconds_config(self):
        """Test time_to_live_in_seconds configuration."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            time_to_live_in_seconds=86400,  # 1 day
        )

        assert config.time_to_live_in_seconds == 86400

    def test_time_to_live_in_seconds_zero(self):
        """Test time_to_live_in_seconds can be set to zero."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            time_to_live_in_seconds=0,
        )

        assert config.time_to_live_in_seconds == 0

    def test_time_to_live_in_seconds_large_value(self):
        """Test time_to_live_in_seconds with large value."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            time_to_live_in_seconds=31536000,  # 1 year in seconds
        )

        assert config.time_to_live_in_seconds == 31536000

    def test_storage_mode_config(self):
        """Test storage_mode configuration."""
        # Default is hot
        config1 = HologresDynamicTableConfig(freshness="1 hours")
        assert config1.storage_mode == "hot"

        # Can be set to cold
        config2 = HologresDynamicTableConfig(
            freshness="1 hours",
            storage_mode="cold",
        )
        assert config2.storage_mode == "cold"

    def test_from_dict_all_fields(self):
        """Test from_dict with all fields populated."""
        config_dict = {
            "freshness": "2 hours",
            "auto_refresh_enable": False,
            "auto_refresh_mode": "incremental",
            "computing_resource": "my_warehouse",
            "base_table_cdc_format": "binlog",
            "partition_key": "event_date",
            "partition_type": "physical",
            "partition_key_time_format": "yyyy-MM-dd",
            "auto_refresh_partition_active_time": "00:00-06:00",
            "orientation": "row",
            "distribution_key": ["user_id", "order_id"],
            "clustering_key": ["created_at"],
            "event_time_column": ["event_time"],
            "bitmap_columns": ["status"],
            "dictionary_encoding_columns": ["category"],
            "time_to_live_in_seconds": 172800,
            "storage_mode": "cold",
        }

        config = HologresDynamicTableConfig.from_dict(config_dict)

        assert config.freshness == "2 hours"
        assert config.auto_refresh_enable is False
        assert config.auto_refresh_mode == "incremental"
        assert config.computing_resource == "my_warehouse"
        assert config.base_table_cdc_format == "binlog"
        assert config.partition_key == "event_date"
        assert config.partition_type == "physical"
        assert config.partition_key_time_format == "yyyy-MM-dd"
        assert config.auto_refresh_partition_active_time == "00:00-06:00"
        assert config.orientation == "row"
        assert config.distribution_key == ["user_id", "order_id"]
        assert config.clustering_key == ["created_at"]
        assert config.event_time_column == ["event_time"]
        assert config.bitmap_columns == ["status"]
        assert config.dictionary_encoding_columns == ["category"]
        assert config.time_to_live_in_seconds == 172800
        assert config.storage_mode == "cold"

    def test_from_dict_partial_fields(self):
        """Test from_dict with partial fields uses defaults."""
        config_dict = {
            "freshness": "30 minutes",
            "orientation": "row",
            "time_to_live_in_seconds": 3600,
        }

        config = HologresDynamicTableConfig.from_dict(config_dict)

        assert config.freshness == "30 minutes"
        assert config.orientation == "row"
        assert config.time_to_live_in_seconds == 3600
        # Defaults should apply
        assert config.auto_refresh_enable is True
        assert config.auto_refresh_mode == "auto"
        assert config.computing_resource == "serverless"
        assert config.storage_mode == "hot"


class TestHologresDynamicTableConfigPartitionFields:
    """Test partition-related fields in HologresDynamicTableConfig."""

    def test_partition_key_single(self):
        """Test single partition key."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            partition_key="ds",
        )

        assert config.partition_key == "ds"
        assert config.partition_type is None

    def test_partition_type_logical(self):
        """Test logical partition type."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            partition_key="ds",
            partition_type="logical",
        )

        assert config.partition_type == "logical"

    def test_partition_type_physical(self):
        """Test physical partition type."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            partition_key="ds",
            partition_type="physical",
        )

        assert config.partition_type == "physical"

    def test_partition_key_time_format(self):
        """Test partition_key_time_format."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            partition_key="ds",
            partition_key_time_format="yyyy-MM-dd",
        )

        assert config.partition_key_time_format == "yyyy-MM-dd"

    def test_auto_refresh_partition_active_time(self):
        """Test auto_refresh_partition_active_time."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            partition_key="ds",
            auto_refresh_partition_active_time="02:00-04:00",
        )

        assert config.auto_refresh_partition_active_time == "02:00-04:00"


class TestHologresDynamicTableConfigTableProperties:
    """Test table property fields in HologresDynamicTableConfig."""

    def test_distribution_key_single(self):
        """Test single distribution key."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            distribution_key=["user_id"],
        )

        assert config.distribution_key == ["user_id"]

    def test_distribution_key_multiple(self):
        """Test multiple distribution keys."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            distribution_key=["user_id", "order_id"],
        )

        assert config.distribution_key == ["user_id", "order_id"]

    def test_clustering_key_single(self):
        """Test single clustering key."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            clustering_key=["created_at"],
        )

        assert config.clustering_key == ["created_at"]

    def test_clustering_key_multiple(self):
        """Test multiple clustering keys."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            clustering_key=["created_at", "updated_at"],
        )

        assert config.clustering_key == ["created_at", "updated_at"]

    def test_event_time_column_single(self):
        """Test single event time column."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            event_time_column=["event_time"],
        )

        assert config.event_time_column == ["event_time"]

    def test_bitmap_columns(self):
        """Test bitmap columns."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            bitmap_columns=["status", "type", "category"],
        )

        assert config.bitmap_columns == ["status", "type", "category"]

    def test_dictionary_encoding_columns(self):
        """Test dictionary encoding columns."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            dictionary_encoding_columns=["country", "region"],
        )

        assert config.dictionary_encoding_columns == ["country", "region"]

    def test_orientation_column(self):
        """Test column orientation (default)."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            orientation="column",
        )

        assert config.orientation == "column"

    def test_orientation_row(self):
        """Test row orientation."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            orientation="row",
        )

        assert config.orientation == "row"


class TestHologresDynamicTableConfigAutoRefresh:
    """Test auto-refresh related fields in HologresDynamicTableConfig."""

    def test_auto_refresh_enable_true(self):
        """Test auto_refresh_enable True."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_enable=True,
        )

        assert config.auto_refresh_enable is True

    def test_auto_refresh_enable_false(self):
        """Test auto_refresh_enable False."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_enable=False,
        )

        assert config.auto_refresh_enable is False

    def test_auto_refresh_mode_auto(self):
        """Test auto_refresh_mode auto."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_mode="auto",
        )

        assert config.auto_refresh_mode == "auto"

    def test_auto_refresh_mode_incremental(self):
        """Test auto_refresh_mode incremental."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_mode="incremental",
        )

        assert config.auto_refresh_mode == "incremental"

    def test_auto_refresh_mode_full(self):
        """Test auto_refresh_mode full."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            auto_refresh_mode="full",
        )

        assert config.auto_refresh_mode == "full"

    def test_computing_resource_serverless(self):
        """Test computing_resource serverless (default)."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            computing_resource="serverless",
        )

        assert config.computing_resource == "serverless"

    def test_computing_resource_local(self):
        """Test computing_resource local."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            computing_resource="local",
        )

        assert config.computing_resource == "local"

    def test_computing_resource_warehouse(self):
        """Test computing_resource with warehouse name."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            computing_resource="my_warehouse",
        )

        assert config.computing_resource == "my_warehouse"

    def test_base_table_cdc_format_stream(self):
        """Test base_table_cdc_format stream (default)."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            base_table_cdc_format="stream",
        )

        assert config.base_table_cdc_format == "stream"

    def test_base_table_cdc_format_binlog(self):
        """Test base_table_cdc_format binlog."""
        config = HologresDynamicTableConfig(
            freshness="1 hours",
            base_table_cdc_format="binlog",
        )

        assert config.base_table_cdc_format == "binlog"
