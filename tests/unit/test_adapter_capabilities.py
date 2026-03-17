"""Unit tests for HologresAdapter capabilities and constraint support."""
import pytest
from unittest import mock

from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.hologres import HologresAdapter
from dbt.adapters.hologres.impl import HologresConfig
from dbt.adapters.hologres.relation_configs import HologresIndexConfig


class TestAdapterCapabilities:
    """Test HologresAdapter capabilities configuration."""

    def test_schema_metadata_by_relations(self):
        """Test SchemaMetadataByRelations capability is fully supported."""
        capabilities = HologresAdapter._capabilities

        assert Capability.SchemaMetadataByRelations in capabilities
        support = capabilities[Capability.SchemaMetadataByRelations]
        assert support.support == Support.Full

    def test_capabilities_dict_structure(self):
        """Test capabilities dict has correct structure."""
        capabilities = HologresAdapter._capabilities

        assert isinstance(capabilities, CapabilityDict)
        # Should have at least one capability
        assert len(capabilities) >= 1

    def test_catalog_by_relation_support_attribute(self):
        """Test CATALOG_BY_RELATION_SUPPORT is True."""
        assert HologresAdapter.CATALOG_BY_RELATION_SUPPORT is True

    def test_capabilities_accessible_from_instance(self):
        """Test capabilities are accessible from adapter instance."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        adapter = HologresAdapter(mock_config, mp_context)

        # Class attribute should be accessible
        assert hasattr(adapter, '_capabilities')
        assert Capability.SchemaMetadataByRelations in adapter._capabilities


class TestConstraintSupport:
    """Test HologresAdapter constraint support configuration."""

    def test_all_constraint_types_supported(self):
        """Test all constraint types are defined in CONSTRAINT_SUPPORT."""
        constraint_support = HologresAdapter.CONSTRAINT_SUPPORT

        # All constraint types should be supported
        assert ConstraintType.check in constraint_support
        assert ConstraintType.not_null in constraint_support
        assert ConstraintType.unique in constraint_support
        assert ConstraintType.primary_key in constraint_support
        assert ConstraintType.foreign_key in constraint_support

    def test_constraint_support_values(self):
        """Test constraint support values are ENFORCED."""
        from dbt.adapters.base import ConstraintSupport

        constraint_support = HologresAdapter.CONSTRAINT_SUPPORT

        # All constraints should be ENFORCED
        for constraint_type in [
            ConstraintType.check,
            ConstraintType.not_null,
            ConstraintType.unique,
            ConstraintType.primary_key,
            ConstraintType.foreign_key,
        ]:
            assert constraint_type in constraint_support
            assert constraint_support[constraint_type] == ConstraintSupport.ENFORCED

    def test_constraint_support_is_dict(self):
        """Test CONSTRAINT_SUPPORT is a dictionary."""
        assert isinstance(HologresAdapter.CONSTRAINT_SUPPORT, dict)

    def test_constraint_support_count(self):
        """Test correct number of constraint types are defined."""
        # Should have 5 constraint types
        assert len(HologresAdapter.CONSTRAINT_SUPPORT) == 5


class TestHologresConfigExtended:
    """Extended tests for HologresConfig dataclass."""

    def test_all_config_fields(self):
        """Test all configuration fields are accessible."""
        config = HologresConfig(
            indexes=[HologresIndexConfig(columns=["col1"])],
            orientation="column",
            distribution_key="user_id",
            clustering_key="event_time",
            event_time_column="created_at",
            segment_key="segment_col",
            bitmap_columns="status,type",
            dictionary_encoding_columns="category",
            logical_partition_key="ds",
        )

        assert config.indexes is not None
        assert config.orientation == "column"
        assert config.distribution_key == "user_id"
        assert config.clustering_key == "event_time"
        assert config.event_time_column == "created_at"
        assert config.segment_key == "segment_col"
        assert config.bitmap_columns == "status,type"
        assert config.dictionary_encoding_columns == "category"
        assert config.logical_partition_key == "ds"

    def test_config_with_indexes_list(self):
        """Test config with multiple indexes."""
        indexes = [
            HologresIndexConfig(columns=["col1"], unique=True),
            HologresIndexConfig(columns=["col2", "col3"], type="bitmap"),
            HologresIndexConfig(columns=["col4"]),
        ]
        config = HologresConfig(indexes=indexes)

        assert len(config.indexes) == 3
        assert config.indexes[0].unique is True
        assert config.indexes[1].type == "bitmap"
        assert config.indexes[2].columns == ["col4"]

    def test_config_optional_fields(self):
        """Test config with only some optional fields set."""
        config = HologresConfig(
            orientation="row",
            distribution_key="id",
        )

        assert config.orientation == "row"
        assert config.distribution_key == "id"
        assert config.indexes is None
        assert config.clustering_key is None
        assert config.event_time_column is None

    def test_config_with_empty_indexes(self):
        """Test config with empty indexes list."""
        config = HologresConfig(indexes=[])

        assert config.indexes == []

    def test_config_distribution_key_various_formats(self):
        """Test distribution_key with various formats."""
        # Single column
        config1 = HologresConfig(distribution_key="user_id")
        assert config1.distribution_key == "user_id"

        # Multiple columns (comma-separated string)
        config2 = HologresConfig(distribution_key="user_id, event_time")
        assert config2.distribution_key == "user_id, event_time"

    def test_config_bitmap_columns_format(self):
        """Test bitmap_columns with various formats."""
        # Single column
        config1 = HologresConfig(bitmap_columns="status")
        assert config1.bitmap_columns == "status"

        # Multiple columns
        config2 = HologresConfig(bitmap_columns="status,type,category")
        assert config2.bitmap_columns == "status,type,category"

    def test_config_logical_partition_key_formats(self):
        """Test logical_partition_key with different formats."""
        # Single key
        config1 = HologresConfig(logical_partition_key="ds")
        assert config1.logical_partition_key == "ds"

        # Two keys
        config2 = HologresConfig(logical_partition_key="yy,mm")
        assert config2.logical_partition_key == "yy,mm"

    def test_config_immutable_defaults(self):
        """Test that default values are consistent across instances."""
        config1 = HologresConfig()
        config2 = HologresConfig()

        # None values should be consistent
        assert config1.indexes is None
        assert config2.indexes is None
        assert config1.orientation is None
        assert config2.orientation is None


class TestAdapterSpecificConfigs:
    """Test AdapterSpecificConfigs class attribute."""

    def test_adapter_specific_configs_is_hologres_config(self):
        """Test AdapterSpecificConfigs points to HologresConfig."""
        assert HologresAdapter.AdapterSpecificConfigs is HologresConfig

    def test_adapter_specific_configs_can_create_instance(self):
        """Test can create config via AdapterSpecificConfigs."""
        config = HologresAdapter.AdapterSpecificConfigs(
            orientation="column"
        )
        assert isinstance(config, HologresConfig)
        assert config.orientation == "column"


class TestAdapterRelationTypes:
    """Test adapter Relation, ConnectionManager, and Column types."""

    def test_relation_type(self):
        """Test Relation type is HologresRelation."""
        from dbt.adapters.hologres.relation import HologresRelation
        assert HologresAdapter.Relation is HologresRelation

    def test_connection_manager_type(self):
        """Test ConnectionManager type is HologresConnectionManager."""
        from dbt.adapters.hologres.connections import HologresConnectionManager
        assert HologresAdapter.ConnectionManager is HologresConnectionManager

    def test_column_type(self):
        """Test Column type is HologresColumn."""
        from dbt.adapters.hologres.column import HologresColumn
        assert HologresAdapter.Column is HologresColumn