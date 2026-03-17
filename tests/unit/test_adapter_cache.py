"""Unit tests for HologresAdapter cache management methods."""
import pytest
from unittest import mock

from dbt.adapters.hologres import HologresAdapter
from dbt.adapters.hologres.impl import GET_RELATIONS_MACRO_NAME
from dbt.adapters.exceptions import CrossDbReferenceProhibitedError, UnexpectedDbReferenceError
from dbt_common.exceptions import DbtRuntimeError


class TestLinkCachedDatabaseRelations:
    """Test HologresAdapter._link_cached_database_relations method."""

    def _create_adapter(self, database="test_db"):
        """Helper to create adapter with mocked dependencies."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = database
        mock_config.credentials.schema = "public"

        adapter = HologresAdapter(mock_config, mp_context)
        adapter._connection_manager = mock.MagicMock()
        adapter.execute = mock.MagicMock(return_value=([], mock.MagicMock()))
        adapter.execute_macro = mock.MagicMock()
        adapter.cache = mock.MagicMock()
        adapter.cache.add_link = mock.MagicMock()

        return adapter

    def test_empty_result(self):
        """Test _link_cached_database_relations with empty result from macro."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = []

        adapter._link_cached_database_relations({"schema1", "schema2"})

        adapter.execute_macro.assert_called_once_with(GET_RELATIONS_MACRO_NAME)
        adapter.cache.add_link.assert_not_called()

    def test_single_relation(self):
        """Test _link_cached_database_relations with single dependency relation."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = [
            ("dep_schema", "dep_view", "ref_schema", "ref_table")
        ]

        adapter._link_cached_database_relations({"ref_schema"})

        adapter.execute_macro.assert_called_once_with(GET_RELATIONS_MACRO_NAME)
        adapter.cache.add_link.assert_called_once()
        # Verify the link was added with correct relations
        call_args = adapter.cache.add_link.call_args
        referenced, dependent = call_args[0]
        assert referenced.schema == "ref_schema"
        assert referenced.identifier == "ref_table"
        assert dependent.schema == "dep_schema"
        assert dependent.identifier == "dep_view"

    def test_multiple_relations(self):
        """Test _link_cached_database_relations with multiple dependency relations."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = [
            ("schema1", "view1", "target_schema", "table1"),
            ("schema2", "view2", "target_schema", "table2"),
            ("schema3", "view3", "other_schema", "table3"),
        ]

        adapter._link_cached_database_relations({"target_schema", "other_schema"})

        assert adapter.cache.add_link.call_count == 3

    def test_cross_schema_reference(self):
        """Test _link_cached_database_relations handles cross-schema references."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = [
            ("analytics", "report_view", "raw", "source_table"),
        ]

        adapter._link_cached_database_relations({"raw"})

        adapter.cache.add_link.assert_called_once()
        call_args = adapter.cache.add_link.call_args
        referenced, dependent = call_args[0]
        assert referenced.schema == "raw"
        assert referenced.identifier == "source_table"

    def test_filters_by_schema(self):
        """Test _link_cached_database_relations filters by schema."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = [
            ("dep_schema", "dep_view", "ref_schema", "ref_table"),
            ("dep_schema2", "dep_view2", "excluded_schema", "ref_table2"),
        ]

        # Only include 'ref_schema' in the schemas set
        adapter._link_cached_database_relations({"ref_schema"})

        # Only the first relation should be linked
        assert adapter.cache.add_link.call_count == 1
        call_args = adapter.cache.add_link.call_args
        referenced, _ = call_args[0]
        assert referenced.schema == "ref_schema"

    def test_case_insensitive_schema(self):
        """Test _link_cached_database_relations handles case-insensitive schema matching."""
        adapter = self._create_adapter()
        adapter.execute_macro.return_value = [
            ("dep_schema", "dep_view", "REF_SCHEMA", "ref_table"),  # uppercase in result
        ]

        # Pass lowercase schema name
        adapter._link_cached_database_relations({"ref_schema"})

        # Should still match due to lower() comparison
        adapter.cache.add_link.assert_called_once()

    def test_database_from_config(self):
        """Test _link_cached_database_relations uses database from config."""
        adapter = self._create_adapter(database="my_database")
        adapter.execute_macro.return_value = [
            ("schema1", "view1", "schema2", "table1"),
        ]

        adapter._link_cached_database_relations({"schema2"})

        call_args = adapter.cache.add_link.call_args
        referenced, dependent = call_args[0]
        assert referenced.database == "my_database"
        assert dependent.database == "my_database"


class TestGetCatalogSchemas:
    """Test HologresAdapter._get_catalog_schemas method."""

    def _create_adapter(self, database="test_db"):
        """Helper to create adapter with mocked dependencies."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = database
        mock_config.credentials.schema = "public"

        adapter = HologresAdapter(mock_config, mp_context)
        adapter._connection_manager = mock.MagicMock()

        return adapter

    def test_single_database(self):
        """Test _get_catalog_schemas with single database."""
        adapter = self._create_adapter()

        # Mock the parent method to return a schema search map
        mock_schema_map = mock.MagicMock()
        mock_schema_map.flatten.return_value = {"test_db": ["schema1", "schema2"]}

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_get_catalog_schemas',
            return_value=mock_schema_map
        ):
            result = adapter._get_catalog_schemas(mock.MagicMock())

        assert result == {"test_db": ["schema1", "schema2"]}
        mock_schema_map.flatten.assert_called_once()

    def test_cross_db_reference_error(self):
        """Test _get_catalog_schemas raises CrossDbReferenceProhibitedError for cross-db reference."""
        adapter = self._create_adapter()

        # Mock the parent method to return a schema search map that raises DbtRuntimeError
        mock_schema_map = mock.MagicMock()
        mock_schema_map.flatten.side_effect = DbtRuntimeError("Cross-database reference not allowed")

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_get_catalog_schemas',
            return_value=mock_schema_map
        ):
            with pytest.raises(CrossDbReferenceProhibitedError):
                adapter._get_catalog_schemas(mock.MagicMock())

    def test_dbruntime_error_wrapped(self):
        """Test _get_catalog_schemas wraps DbtRuntimeError in CrossDbReferenceProhibitedError."""
        adapter = self._create_adapter()

        mock_schema_map = mock.MagicMock()
        mock_schema_map.flatten.side_effect = DbtRuntimeError("Some error message")

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_get_catalog_schemas',
            return_value=mock_schema_map
        ):
            with pytest.raises(CrossDbReferenceProhibitedError) as exc_info:
                adapter._get_catalog_schemas(mock.MagicMock())

            assert "Some error message" in str(exc_info.value)


class TestRelationsCacheForSchemas:
    """Test HologresAdapter._relations_cache_for_schemas method."""

    def _create_adapter(self, database="test_db"):
        """Helper to create adapter with mocked dependencies."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = database
        mock_config.credentials.schema = "public"

        adapter = HologresAdapter(mock_config, mp_context)
        adapter._connection_manager = mock.MagicMock()
        adapter.execute = mock.MagicMock(return_value=([], mock.MagicMock()))
        adapter.execute_macro = mock.MagicMock(return_value=[])
        adapter.cache = mock.MagicMock()
        adapter.cache.add_link = mock.MagicMock()

        return adapter

    def test_calls_parent_method(self):
        """Test _relations_cache_for_schemas calls parent method."""
        adapter = self._create_adapter()
        mock_manifest = mock.MagicMock()

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_relations_cache_for_schemas'
        ) as mock_parent:
            adapter._relations_cache_for_schemas(mock_manifest)

            mock_parent.assert_called_once_with(mock_manifest, None)

    def test_calls_parent_method_with_cache_schemas(self):
        """Test _relations_cache_for_schemas passes cache_schemas parameter."""
        adapter = self._create_adapter()
        mock_manifest = mock.MagicMock()
        cache_schemas = [mock.MagicMock()]

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_relations_cache_for_schemas'
        ) as mock_parent:
            adapter._relations_cache_for_schemas(mock_manifest, cache_schemas)

            mock_parent.assert_called_once_with(mock_manifest, cache_schemas)

    def test_links_relations(self):
        """Test _relations_cache_for_schemas calls _link_cached_relations."""
        adapter = self._create_adapter()
        mock_manifest = mock.MagicMock()

        # Mock the internal methods
        adapter._get_cache_schemas = mock.MagicMock(return_value=[])
        adapter._link_cached_database_relations = mock.MagicMock()

        with mock.patch.object(
            HologresAdapter.__bases__[0],
            '_relations_cache_for_schemas'
        ):
            adapter._relations_cache_for_schemas(mock_manifest)

            # Verify that _link_cached_relations was called (via _link_cached_database_relations)
            adapter._link_cached_database_relations.assert_called()


class TestLinkCachedRelations:
    """Test HologresAdapter._link_cached_relations method."""

    def _create_adapter(self, database="test_db"):
        """Helper to create adapter with mocked dependencies."""
        from multiprocessing.context import SpawnContext
        mp_context = SpawnContext()
        mock_config = mock.MagicMock()
        mock_config.credentials.database = database
        mock_config.credentials.schema = "public"

        adapter = HologresAdapter(mock_config, mp_context)
        adapter._connection_manager = mock.MagicMock()
        adapter.execute = mock.MagicMock(return_value=([], mock.MagicMock()))
        adapter.execute_macro = mock.MagicMock(return_value=[])
        adapter.cache = mock.MagicMock()
        adapter.cache.add_link = mock.MagicMock()

        return adapter

    def test_empty_schemas(self):
        """Test _link_cached_relations with empty schemas."""
        adapter = self._create_adapter()

        # Mock _get_cache_schemas to return empty list
        mock_relation = mock.MagicMock()
        mock_relation.schema = "test_schema"
        adapter._get_cache_schemas = mock.MagicMock(return_value=[])

        # Should not raise any error
        adapter._link_cached_relations(mock.MagicMock())

    def test_validates_all_databases(self):
        """Test _link_cached_relations validates database for all relations."""
        adapter = self._create_adapter(database="expected_db")

        # Create mock relations
        mock_relation1 = mock.MagicMock()
        mock_relation1.database = "expected_db"
        mock_relation1.schema = "schema1"

        mock_relation2 = mock.MagicMock()
        mock_relation2.database = "expected_db"
        mock_relation2.schema = "schema2"

        adapter._get_cache_schemas = mock.MagicMock(
            return_value=[mock_relation1, mock_relation2]
        )
        adapter._link_cached_database_relations = mock.MagicMock()

        mock_manifest = mock.MagicMock()
        adapter._link_cached_relations(mock_manifest)

        # Verify verify_database was called for each relation
        # The method should complete without raising an error
        adapter._link_cached_database_relations.assert_called_once()

    def test_skips_invalid_database(self):
        """Test _link_cached_relations raises error for invalid database."""
        adapter = self._create_adapter(database="correct_db")

        # Create a mock relation with wrong database
        mock_relation = mock.MagicMock()
        mock_relation.database = "wrong_db"
        mock_relation.schema = "schema1"

        adapter._get_cache_schemas = mock.MagicMock(return_value=[mock_relation])

        mock_manifest = mock.MagicMock()

        # Should raise UnexpectedDbReferenceError
        with pytest.raises(UnexpectedDbReferenceError):
            adapter._link_cached_relations(mock_manifest)

    def test_schema_lowercased(self):
        """Test _link_cached_relations lowercases schema names."""
        adapter = self._create_adapter(database="test_db")

        # Create a mock relation with uppercase schema
        mock_relation = mock.MagicMock()
        mock_relation.database = "test_db"
        mock_relation.schema = "UPPER_SCHEMA"

        adapter._get_cache_schemas = mock.MagicMock(return_value=[mock_relation])
        adapter._link_cached_database_relations = mock.MagicMock()

        mock_manifest = mock.MagicMock()
        adapter._link_cached_relations(mock_manifest)

        # Verify that the schema was lowercased when added to the set
        call_args = adapter._link_cached_database_relations.call_args
        schemas = call_args[0][0]
        assert "upper_schema" in schemas

    def test_multiple_schemas_deduplicated(self):
        """Test _link_cached_relations deduplicates schemas."""
        adapter = self._create_adapter(database="test_db")

        # Create mock relations with duplicate schemas (different cases)
        mock_relation1 = mock.MagicMock()
        mock_relation1.database = "test_db"
        mock_relation1.schema = "SCHEMA_A"

        mock_relation2 = mock.MagicMock()
        mock_relation2.database = "test_db"
        mock_relation2.schema = "schema_a"  # Same schema, different case

        adapter._get_cache_schemas = mock.MagicMock(
            return_value=[mock_relation1, mock_relation2]
        )
        adapter._link_cached_database_relations = mock.MagicMock()

        mock_manifest = mock.MagicMock()
        adapter._link_cached_relations(mock_manifest)

        # Verify schemas are deduplicated (lowercase)
        call_args = adapter._link_cached_database_relations.call_args
        schemas = call_args[0][0]
        assert len(schemas) == 1
        assert "schema_a" in schemas