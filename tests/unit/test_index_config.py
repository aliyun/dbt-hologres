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
