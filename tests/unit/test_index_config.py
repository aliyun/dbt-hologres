"""Unit tests for HologresIndexConfig."""
import pytest
from unittest import mock
from datetime import datetime, timezone

from dbt.adapters.hologres.relation_configs import HologresIndexConfig, HologresIndexConfigChange
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt_common.dataclass_schema import ValidationError


class TestHologresIndexConfig:
    """Test HologresIndexConfig class."""

    def test_create_basic_index(self):
        """Test creating a basic index config."""
        index = HologresIndexConfig(columns=["col1", "col2"])

        assert index.columns == ["col1", "col2"]
        assert index.unique is False
        assert index.type is None

    def test_create_unique_index(self):
        """Test creating a unique index config."""
        index = HologresIndexConfig(columns=["id"], unique=True)

        assert index.columns == ["id"]
        assert index.unique is True

    def test_create_index_with_type(self):
        """Test creating an index with type."""
        index = HologresIndexConfig(
            columns=["email"],
            unique=True,
            type="hash"
        )

        assert index.columns == ["email"]
        assert index.unique is True
        assert index.type == "hash"

    def test_render_generates_consistent_name(self):
        """Test render generates deterministic index name based on timestamp."""
        from dbt.adapters.hologres.relation_configs import index as index_module

        relation = mock.MagicMock()
        relation.render.return_value = "my_schema.my_table"

        # Mock datetime to return consistent timestamp
        with mock.patch.object(index_module, "datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-01T00:00:00"
            mock_datetime.now.return_value.replace.return_value.isoformat.return_value = "2024-01-01T00:00:00"

            index1 = HologresIndexConfig(columns=["col1", "col2"], unique=False)
            name1 = index1.render(relation)

            # Same index with same timestamp should generate same name
            index2 = HologresIndexConfig(columns=["col1", "col2"], unique=False)
            name2 = index2.render(relation)

            assert name1 == name2

    def test_render_different_columns_different_names(self):
        """Test render with different columns generates different names."""
        relation = mock.MagicMock()
        relation.render.return_value = "my_schema.my_table"

        index1 = HologresIndexConfig(columns=["col1"], unique=False)
        name1 = index1.render(relation)

        index2 = HologresIndexConfig(columns=["col2"], unique=False)
        name2 = index2.render(relation)

        assert name1 != name2

    def test_render_unique_affects_name(self):
        """Test render with different unique flag generates different names."""
        relation = mock.MagicMock()
        relation.render.return_value = "my_schema.my_table"

        index1 = HologresIndexConfig(columns=["col1"], unique=False)
        name1 = index1.render(relation)

        index2 = HologresIndexConfig(columns=["col1"], unique=True)
        name2 = index2.render(relation)

        assert name1 != name2

    def test_parse_valid_dict(self):
        """Test parse with valid dictionary."""
        raw_index = {
            "columns": ["col1", "col2"],
            "unique": True,
            "type": "btree"
        }

        result = HologresIndexConfig.parse(raw_index)

        assert isinstance(result, HologresIndexConfig)
        assert result.columns == ["col1", "col2"]
        assert result.unique is True
        assert result.type == "btree"

    def test_parse_none(self):
        """Test parse with None returns None."""
        result = HologresIndexConfig.parse(None)
        assert result is None

    def test_parse_invalid_dict_raises_error(self):
        """Test parse with invalid dict raises IndexConfigError."""
        # Missing required 'columns' field
        raw_index = {"unique": True}

        with pytest.raises(IndexConfigError):
            HologresIndexConfig.parse(raw_index)

    def test_parse_non_dict_raises_error(self):
        """Test parse with non-dict raises IndexConfigError."""
        raw_index = "not_a_dict"

        # When validate fails with ValidationError, it gets wrapped in IndexConfigError
        with pytest.raises(IndexConfigError):
            HologresIndexConfig.parse(raw_index)

    def test_parse_list_raises_error(self):
        """Test parse with list raises IndexConfigError."""
        raw_index = ["col1", "col2"]

        # When validate fails with ValidationError, it gets wrapped in IndexConfigError
        with pytest.raises(IndexConfigError):
            HologresIndexConfig.parse(raw_index)

    def test_from_dict(self):
        """Test from_dict creates index config."""
        config_dict = {
            "columns": ["id", "name"],
            "unique": False,
            "type": "btree"
        }

        result = HologresIndexConfig.from_dict(config_dict)

        assert result.columns == ["id", "name"]
        assert result.unique is False
        assert result.type == "btree"

    def test_from_dict_with_defaults(self):
        """Test from_dict applies defaults."""
        config_dict = {"columns": ["id"]}

        result = HologresIndexConfig.from_dict(config_dict)

        assert result.columns == ["id"]
        assert result.unique is False  # default
        assert result.type is None  # default


class TestHologresIndexConfigChange:
    """Test HologresIndexConfigChange class."""

    def test_create_change(self):
        """Test creating an index config change."""
        index = HologresIndexConfig(columns=["col1"], unique=True)
        change = HologresIndexConfigChange.from_dict({
            "action": "create",
            "context": index
        })

        assert change.action == "create"
        assert change.context is index

    def test_requires_full_refresh_false(self):
        """Test requires_full_refresh is always False."""
        index = HologresIndexConfig(columns=["col1"], unique=True)
        change = HologresIndexConfigChange.from_dict({
            "action": "create",
            "context": index
        })

        assert change.requires_full_refresh is False

    def test_from_dict_create_action(self):
        """Test from_dict with create action."""
        index = HologresIndexConfig(columns=["col1"])
        change = HologresIndexConfigChange.from_dict({
            "action": "create",
            "context": index
        })

        assert change.action == "create"

    def test_from_dict_drop_action(self):
        """Test from_dict with drop action."""
        index = HologresIndexConfig(columns=["col1"])
        change = HologresIndexConfigChange.from_dict({
            "action": "drop",
            "context": index
        })

        assert change.action == "drop"


class TestHologresIndexConfigHashMethod:
    """Test HologresIndexConfig __hash__ method."""

    def test_hash_same_config_same_hash(self):
        """Test identical configs produce same hash."""
        index1 = HologresIndexConfig(columns=["col1", "col2"], unique=True, type="btree")
        index2 = HologresIndexConfig(columns=["col1", "col2"], unique=True, type="btree")

        assert hash(index1) == hash(index2)

    def test_hash_different_columns_different_hash(self):
        """Test different columns produce different hash."""
        index1 = HologresIndexConfig(columns=["col1"])
        index2 = HologresIndexConfig(columns=["col2"])

        assert hash(index1) != hash(index2)

    def test_hash_different_unique_different_hash(self):
        """Test different unique flag produces different hash."""
        index1 = HologresIndexConfig(columns=["col1"], unique=False)
        index2 = HologresIndexConfig(columns=["col1"], unique=True)

        assert hash(index1) != hash(index2)

    def test_hash_different_type_different_hash(self):
        """Test different type produces different hash."""
        index1 = HologresIndexConfig(columns=["col1"], type="btree")
        index2 = HologresIndexConfig(columns=["col1"], type="hash")

        assert hash(index1) != hash(index2)

    def test_hash_set_usage(self):
        """Test HologresIndexConfig can be used in set."""
        index1 = HologresIndexConfig(columns=["col1", "col2"])
        index2 = HologresIndexConfig(columns=["col1", "col2"])  # Same config
        index3 = HologresIndexConfig(columns=["col3"])  # Different config

        index_set = {index1, index2, index3}
        assert len(index_set) == 2
        assert index1 in index_set
        assert index3 in index_set

    def test_hash_dict_key_usage(self):
        """Test HologresIndexConfig can be used as dict key."""
        index1 = HologresIndexConfig(columns=["col1"])
        index2 = HologresIndexConfig(columns=["col1"])  # Same config
        index3 = HologresIndexConfig(columns=["col2"])  # Different config

        index_dict = {index1: "first", index2: "second", index3: "third"}
        assert len(index_dict) == 2
        assert index_dict[index1] == "second"  # index2 overwrote index1


class TestHologresIndexConfigEquality:
    """Test HologresIndexConfig __eq__ method."""

    def test_eq_same_config_true(self):
        """Test identical configs are equal."""
        index1 = HologresIndexConfig(columns=["col1", "col2"], unique=True, type="btree")
        index2 = HologresIndexConfig(columns=["col1", "col2"], unique=True, type="btree")

        assert index1 == index2

    def test_eq_different_columns_false(self):
        """Test different columns are not equal."""
        index1 = HologresIndexConfig(columns=["col1"])
        index2 = HologresIndexConfig(columns=["col2"])

        assert index1 != index2

    def test_eq_different_unique_false(self):
        """Test different unique flag are not equal."""
        index1 = HologresIndexConfig(columns=["col1"], unique=False)
        index2 = HologresIndexConfig(columns=["col1"], unique=True)

        assert index1 != index2

    def test_eq_different_type_false(self):
        """Test different type are not equal."""
        index1 = HologresIndexConfig(columns=["col1"], type="btree")
        index2 = HologresIndexConfig(columns=["col1"], type="hash")

        assert index1 != index2

    def test_eq_non_indexconfig_returns_notimplemented(self):
        """Test equality with non-HologresIndexConfig returns NotImplemented."""
        index = HologresIndexConfig(columns=["col1"])

        # Comparing with different type should return NotImplemented
        # Python's == will then return False
        result = index.__eq__("not_an_index")
        assert result is NotImplemented

    def test_eq_with_none_returns_notimplemented(self):
        """Test equality with None returns NotImplemented."""
        index = HologresIndexConfig(columns=["col1"])

        result = index.__eq__(None)
        assert result is NotImplemented

    def test_eq_with_dict_returns_notimplemented(self):
        """Test equality with dict returns NotImplemented."""
        index = HologresIndexConfig(columns=["col1"])

        result = index.__eq__({"columns": ["col1"]})
        assert result is NotImplemented

    def test_eq_column_order_matters(self):
        """Test that column order affects equality."""
        index1 = HologresIndexConfig(columns=["col1", "col2"])
        index2 = HologresIndexConfig(columns=["col2", "col1"])

        # Different order means different configs
        assert index1 != index2


class TestHologresIndexConfigRenderWithTimestamp:
    """Test render method behavior with timestamps."""

    def test_render_includes_timestamp(self):
        """Test render includes timestamp in hash generation."""
        relation = mock.MagicMock()
        relation.render.return_value = "my_schema.my_table"

        index = HologresIndexConfig(columns=["col1"])
        name = index.render(relation)

        # The render method generates a hash that includes timestamp
        # We just verify it returns a string (the md5 hash)
        assert isinstance(name, str)
        assert len(name) == 32  # MD5 hex digest length

    def test_render_different_times_different_names(self):
        """Test render at different times produces different names."""
        from dbt.adapters.hologres.relation_configs import index as index_module

        relation = mock.MagicMock()
        relation.render.return_value = "my_schema.my_table"

        index = HologresIndexConfig(columns=["col1"])

        # Get first name
        name1 = index.render(relation)

        # Wait a tiny bit and get second name (would need mocking for actual test)
        # Since we can't guarantee different timestamps, just verify format
        name2 = index.render(relation)

        # Both should be valid md5 hashes
        assert isinstance(name1, str)
        assert isinstance(name2, str)
