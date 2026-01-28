"""Unit tests for HologresColumn."""
import pytest
from unittest import mock

from dbt.adapters.hologres.column import HologresColumn


class TestHologresColumn:
    """Test HologresColumn class."""

    def test_data_type_text(self):
        """Test data_type preserves text type."""
        column = HologresColumn(
            column="test_col",
            dtype="text",
        )

        assert column.data_type == "text"

    def test_data_type_varchar_without_size(self):
        """Test data_type preserves varchar without size."""
        column = HologresColumn(
            column="test_col",
            dtype="character varying",
        )

        assert column.data_type == "character varying"

    def test_data_type_varchar_with_size(self):
        """Test data_type converts varchar with size."""
        column = HologresColumn(
            column="test_col",
            dtype="character varying",
            char_size=255,
        )

        # Should call parent class and get character varying(255)
        assert "varying" in column.data_type
        assert "255" in column.data_type

    def test_data_type_text_case_insensitive(self):
        """Test data_type handles TEXT case insensitively."""
        column = HologresColumn(
            column="test_col",
            dtype="TEXT",
        )

        assert column.data_type.lower() == "text"

    def test_data_type_character_varying_case_insensitive(self):
        """Test data_type handles character varying case insensitively."""
        column = HologresColumn(
            column="test_col",
            dtype="CHARACTER VARYING",
        )

        assert column.data_type.lower() == "character varying"

    def test_data_type_integer(self):
        """Test data_type for integer types."""
        column = HologresColumn(
            column="test_col",
            dtype="integer",
        )

        assert column.data_type == "integer"

    def test_data_type_timestamp(self):
        """Test data_type for timestamp types."""
        column = HologresColumn(
            column="test_col",
            dtype="timestamp without time zone",
        )

        assert column.data_type == "timestamp without time zone"

    def test_data_type_boolean(self):
        """Test data_type for boolean types."""
        column = HologresColumn(
            column="test_col",
            dtype="boolean",
        )

        assert column.data_type == "boolean"
