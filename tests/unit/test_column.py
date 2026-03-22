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


class TestHologresColumnDataTypeExtended:
    """Extended tests for HologresColumn data_type property."""

    def test_text_type_not_converted(self):
        """Test TEXT type is not converted to varchar."""
        column = HologresColumn(
            column="test_col",
            dtype="text",
        )

        # TEXT should remain as "text", not converted to varchar
        assert column.data_type == "text"
        assert "varchar" not in column.data_type.lower()

    def test_varchar_with_size(self):
        """Test VARCHAR with size is formatted correctly."""
        column = HologresColumn(
            column="test_col",
            dtype="character varying",
            char_size=255,
        )

        # Should include the size
        assert "255" in column.data_type

    def test_varchar_without_size(self):
        """Test VARCHAR without size remains as character varying."""
        column = HologresColumn(
            column="test_col",
            dtype="character varying",
        )

        # Should remain as "character varying" without size
        assert column.data_type == "character varying"

    def test_hologres_specific_types_roaringbitmap(self):
        """Test Hologres specific type: ROARINGBITMAP."""
        column = HologresColumn(
            column="bitmap_col",
            dtype="roaringbitmap",
        )

        # Should pass through as-is
        assert column.data_type == "roaringbitmap"

    def test_hologres_specific_types_json(self):
        """Test Hologres JSON type."""
        column = HologresColumn(
            column="json_col",
            dtype="json",
        )

        assert column.data_type == "json"

    def test_hologres_specific_types_jsonb(self):
        """Test Hologres JSONB type."""
        column = HologresColumn(
            column="jsonb_col",
            dtype="jsonb",
        )

        assert column.data_type == "jsonb"

    def test_numeric_with_precision(self):
        """Test NUMERIC type with precision."""
        column = HologresColumn(
            column="amount",
            dtype="numeric",
            numeric_precision=10,
            numeric_scale=2,
        )

        # Should be formatted with precision and scale
        assert "numeric" in column.data_type
        assert "10" in column.data_type
        assert "2" in column.data_type

    def test_numeric_without_precision(self):
        """Test NUMERIC type without precision."""
        column = HologresColumn(
            column="amount",
            dtype="numeric",
        )

        assert column.data_type == "numeric"

    def test_bigint_type(self):
        """Test BIGINT type."""
        column = HologresColumn(
            column="id",
            dtype="bigint",
        )

        assert column.data_type == "bigint"

    def test_smallint_type(self):
        """Test SMALLINT type."""
        column = HologresColumn(
            column="small_id",
            dtype="smallint",
        )

        assert column.data_type == "smallint"

    def test_real_type(self):
        """Test REAL (float4) type."""
        column = HologresColumn(
            column="float_val",
            dtype="real",
        )

        assert column.data_type == "real"

    def test_double_precision_type(self):
        """Test DOUBLE PRECISION (float8) type."""
        column = HologresColumn(
            column="double_val",
            dtype="double precision",
        )

        assert column.data_type == "double precision"

    def test_date_type(self):
        """Test DATE type."""
        column = HologresColumn(
            column="date_col",
            dtype="date",
        )

        assert column.data_type == "date"

    def test_time_type(self):
        """Test TIME type."""
        column = HologresColumn(
            column="time_col",
            dtype="time without time zone",
        )

        assert column.data_type == "time without time zone"

    def test_timetz_type(self):
        """Test TIME WITH TIME ZONE type."""
        column = HologresColumn(
            column="timetz_col",
            dtype="time with time zone",
        )

        assert column.data_type == "time with time zone"

    def test_timestamptz_type(self):
        """Test TIMESTAMP WITH TIME ZONE type."""
        column = HologresColumn(
            column="timestamptz_col",
            dtype="timestamp with time zone",
        )

        assert column.data_type == "timestamp with time zone"

    def test_array_type(self):
        """Test ARRAY type."""
        column = HologresColumn(
            column="array_col",
            dtype="integer[]",
        )

        assert column.data_type == "integer[]"

    def test_text_array_type(self):
        """Test TEXT[] array type."""
        column = HologresColumn(
            column="text_array",
            dtype="text[]",
        )

        assert column.data_type == "text[]"

    def test_case_insensitive_text(self):
        """Test TEXT type is case insensitive."""
        for dtype in ["TEXT", "Text", "tExT"]:
            column = HologresColumn(column="col", dtype=dtype)
            assert column.data_type.lower() == "text"

    def test_case_insensitive_character_varying(self):
        """Test CHARACTER VARYING is case insensitive."""
        for dtype in ["CHARACTER VARYING", "Character Varying", "character varying"]:
            column = HologresColumn(column="col", dtype=dtype)
            assert column.data_type.lower() == "character varying"

    def test_decimal_type(self):
        """Test DECIMAL type (alias for NUMERIC)."""
        column = HologresColumn(
            column="decimal_col",
            dtype="decimal",
        )

        assert column.data_type == "decimal"

    def test_serial_type(self):
        """Test SERIAL type."""
        column = HologresColumn(
            column="serial_col",
            dtype="serial",
        )

        assert column.data_type == "serial"

    def test_bigserial_type(self):
        """Test BIGSERIAL type."""
        column = HologresColumn(
            column="bigserial_col",
            dtype="bigserial",
        )

        assert column.data_type == "bigserial"

    def test_bpchar_type(self):
        """Test BPCHAR (blank padded character) type."""
        column = HologresColumn(
            column="bpchar_col",
            dtype="bpchar",
        )

        assert column.data_type == "bpchar"

    def test_char_type(self):
        """Test CHAR type with size."""
        column = HologresColumn(
            column="char_col",
            dtype="character",
            char_size=10,
        )

        # Should include size in the type
        assert "10" in column.data_type


class TestHologresColumnNameProperty:
    """Tests for HologresColumn name property."""

    def test_name_property(self):
        """Test name property returns column name."""
        column = HologresColumn(
            column="test_column",
            dtype="text",
        )

        assert column.name == "test_column"

    def test_column_and_name_same(self):
        """Test column and name properties are the same."""
        column = HologresColumn(
            column="my_col",
            dtype="integer",
        )

        assert column.column == column.name


class TestHologresColumnWithNullableInfo:
    """Tests for HologresColumn with nullable information."""

    def test_column_without_table_info(self):
        """Test column can be created without table info."""
        column = HologresColumn(
            column="standalone_col",
            dtype="varchar",
        )

        assert column.column == "standalone_col"
        assert column.dtype == "varchar"


class TestHologresColumnDataTypeVarcharEdgeCases:
    """Edge case tests for VARCHAR type handling."""

    def test_varchar_with_large_size(self):
        """Test VARCHAR with large size value."""
        column = HologresColumn(
            column="large_text",
            dtype="character varying",
            char_size=65535,
        )

        assert "65535" in column.data_type

    def test_varchar_with_size_one(self):
        """Test VARCHAR with size 1."""
        column = HologresColumn(
            column="single_char",
            dtype="character varying",
            char_size=1,
        )

        assert "1" in column.data_type

    def test_varchar_different_dtype_names(self):
        """Test that different VARCHAR dtype names are handled."""
        # "character varying" is the standard PostgreSQL/Hologres name
        column = HologresColumn(
            column="col",
            dtype="character varying",
        )

        # Without size, should return dtype as-is
        assert column.data_type == "character varying"

    def test_varchar_with_size_preserves_dtype(self):
        """Test VARCHAR with size includes dtype name."""
        column = HologresColumn(
            column="col",
            dtype="character varying",
            char_size=100,
        )

        # Should include both "varying" and "100"
        assert "varying" in column.data_type.lower()
        assert "100" in column.data_type


class TestHologresColumnTextTypePreserved:
    """Tests verifying TEXT type is preserved and not converted."""

    def test_text_not_converted_to_varchar(self):
        """Test TEXT is not converted to VARCHAR without size."""
        column = HologresColumn(
            column="text_col",
            dtype="text",
        )

        # TEXT should stay as "text", not "character varying"
        assert column.data_type == "text"
        assert "varying" not in column.data_type

    def test_text_with_char_size_ignored(self):
        """Test TEXT with char_size still returns text (no size added)."""
        column = HologresColumn(
            column="text_col",
            dtype="text",
            char_size=255,  # This should be ignored for text type
        )

        # TEXT should stay as "text" even with char_size
        assert column.data_type == "text"

    def test_text_case_variations(self):
        """Test TEXT type is recognized regardless of case."""
        for dtype in ["TEXT", "Text", "tExT", "TeXt"]:
            column = HologresColumn(column="col", dtype=dtype)
            assert column.data_type.lower() == "text"


class TestHologresColumnOtherTypes:
    """Tests for other data types that use parent class logic."""

    def test_other_types_use_parent_logic(self):
        """Test that non-text/non-varchar types use parent class logic."""
        # These types should go through parent class data_type property
        column = HologresColumn(
            column="int_col",
            dtype="integer",
        )

        # Should return the dtype from parent
        assert column.data_type == "integer"

    def test_other_types_with_numeric_precision(self):
        """Test numeric types with precision use parent logic."""
        column = HologresColumn(
            column="decimal_col",
            dtype="numeric",
            numeric_precision=10,
            numeric_scale=2,
        )

        # Parent class should format this
        assert "numeric" in column.data_type

    def test_other_types_with_char_size(self):
        """Test char type with size uses parent logic."""
        column = HologresColumn(
            column="char_col",
            dtype="character",
            char_size=10,
        )

        # Parent class should format this with size
        assert "10" in column.data_type


class TestHologresColumnMixedCase:
    """Tests for case-insensitive dtype handling."""

    def test_mixed_case_character_varying(self):
        """Test mixed case 'Character Varying' is handled."""
        column = HologresColumn(
            column="col",
            dtype="Character Varying",
        )

        # Should preserve the original case in output
        assert column.data_type.lower() == "character varying"

    def test_mixed_case_character_varying_with_size(self):
        """Test mixed case 'Character Varying' with size."""
        column = HologresColumn(
            column="col",
            dtype="Character Varying",
            char_size=50,
        )

        # Should include size
        assert "50" in column.data_type

    def test_upper_case_text(self):
        """Test upper case 'TEXT' is handled correctly."""
        column = HologresColumn(
            column="col",
            dtype="TEXT",
        )

        assert column.data_type == "TEXT"


class TestHologresColumnHologresSpecificTypes:
    """Tests for Hologres-specific data types."""

    def test_roaringbitmap_type(self):
        """Test ROARINGBITMAP type is passed through."""
        column = HologresColumn(
            column="bitmap_col",
            dtype="roaringbitmap",
        )

        assert column.data_type == "roaringbitmap"

    def test_hll_type(self):
        """Test HLL (HyperLogLog) type is passed through."""
        column = HologresColumn(
            column="hll_col",
            dtype="hll",
        )

        assert column.data_type == "hll"

    def test_bitor_agg_type(self):
        """Test BITOR_AGG type is passed through."""
        column = HologresColumn(
            column="bitor_col",
            dtype="bitor_agg",
        )

        assert column.data_type == "bitor_agg"
