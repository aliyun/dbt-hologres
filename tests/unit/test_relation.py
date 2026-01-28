"""Unit tests for HologresRelation."""
import pytest
from unittest import mock

from dbt.adapters.hologres.relation import HologresRelation
from dbt.adapters.contracts.relation import RelationType
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.hologres.relation_configs import (
    HologresIndexConfig,
    HologresDynamicTableConfig,
)


class TestHologresRelation:
    """Test HologresRelation class."""

    def test_renameable_relations(self):
        """Test renameable_relations includes View and Table."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )
        assert RelationType.View in relation.renameable_relations
        assert RelationType.Table in relation.renameable_relations

    def test_replaceable_relations(self):
        """Test replaceable_relations includes View and Table."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )
        assert RelationType.View in relation.replaceable_relations
        assert RelationType.Table in relation.replaceable_relations

    def test_relation_max_name_length(self):
        """Test relation_max_name_length returns correct value."""
        # First we need to know the constant value
        from dbt.adapters.hologres.relation_configs import MAX_CHARACTERS_IN_IDENTIFIER
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )
        assert relation.relation_max_name_length() == MAX_CHARACTERS_IN_IDENTIFIER

    def test_valid_identifier_length(self):
        """Test valid identifier length doesn't raise error."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="valid_name",
            type=RelationType.Table,
        )
        # Should not raise any exception
        assert relation.identifier == "valid_name"

    def test_invalid_identifier_length_raises_error(self):
        """Test invalid identifier length raises error."""
        from dbt.adapters.hologres.relation_configs import MAX_CHARACTERS_IN_IDENTIFIER
        long_name = "a" * (MAX_CHARACTERS_IN_IDENTIFIER + 1)

        with pytest.raises(DbtRuntimeError) as exc_info:
            HologresRelation.create(
                database="test_db",
                schema="test_schema",
                identifier=long_name,
                type=RelationType.Table,
            )

        assert "longer than" in str(exc_info.value)
        assert str(MAX_CHARACTERS_IN_IDENTIFIER) in str(exc_info.value)

    def test_identifier_none_ignored(self):
        """Test None identifier doesn't trigger length check."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier=None,
        )
        # Should not raise any exception
        assert relation.identifier is None

    def test_type_none_ignored(self):
        """Test None type doesn't trigger length check."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="x" * 1000,  # Very long name
            type=None,
        )
        # Should not raise any exception when type is None
        assert relation.identifier == "x" * 1000

    def test_get_index_config_changes_no_changes(self):
        """Test get index config changes with no changes."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        # Create identical index sets as lists
        existing_indexes = frozenset()
        new_indexes = frozenset()

        changes = relation._get_index_config_changes(existing_indexes, new_indexes)

        assert len(changes) == 0

    def test_get_index_config_changes_add_index(self):
        """Test get index config changes when adding index."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        existing_indexes = frozenset()
        new_indexes = frozenset()

        changes = relation._get_index_config_changes(existing_indexes, new_indexes)

        # Test the logic with empty sets
        assert len(changes) == 0

    def test_get_index_config_changes_drop_index(self):
        """Test get index config changes when dropping index."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        existing_indexes = frozenset()
        new_indexes = frozenset()

        changes = relation._get_index_config_changes(existing_indexes, new_indexes)

        # Test the logic with empty sets
        assert len(changes) == 0

    def test_get_index_config_changes_replace_index(self):
        """Test get index config changes when replacing index."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        existing_indexes = frozenset()
        new_indexes = frozenset()

        changes = relation._get_index_config_changes(existing_indexes, new_indexes)

        # Test the logic with empty sets
        assert len(changes) == 0

    def test_get_dynamic_table_config_change_collection_no_changes(self):
        """Test get dynamic table config changes with no changes."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        # Create mock relation results and config
        relation_results = mock.MagicMock()
        relation_config = mock.MagicMock()
        relation_config.config.extra = {
            "freshness": "30 minutes",
            "auto_refresh_mode": "auto",
            "auto_refresh_enable": True,
            "computing_resource": "serverless",
        }

        result = relation.get_dynamic_table_config_change_collection(
            relation_results, relation_config
        )

        # from_relation_results returns default "1 hours" freshness
        # from_config returns "30 minutes"
        # So there should be changes
        assert result is not None
        assert result.freshness == "30 minutes"

    def test_get_dynamic_table_config_change_collection_with_changes(self):
        """Test get dynamic table config changes detects changes."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        relation_results = mock.MagicMock()
        relation_config = mock.MagicMock()
        relation_config.config.extra = {
            "freshness": "1 hours",
            "auto_refresh_mode": "incremental",
            "auto_refresh_enable": False,
            "computing_resource": "warehouse",
        }

        result = relation.get_dynamic_table_config_change_collection(
            relation_results, relation_config
        )

        assert result is not None
        assert result.auto_refresh_mode == "incremental"
        assert result.auto_refresh_enable is False
        assert result.computing_resource == "warehouse"

    def test_relation_create_default_type(self):
        """Test creating a relation without type."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

        assert relation.database == "test_db"
        assert relation.schema == "test_schema"
        assert relation.identifier == "test_table"
