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
