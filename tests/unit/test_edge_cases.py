"""Unit tests for edge cases and boundary conditions in dbt-hologres.

These tests verify behavior at the boundaries and edge cases for
LocalDate, Credentials, and Configuration handling.
"""
import pytest
from unittest import mock
from datetime import date

from dbt.adapters.hologres.local_date import LocalDate, parse_date, today
from dbt.adapters.hologres.connections import HologresCredentials
from dbt.adapters.hologres.impl import HologresConfig
from dbt.adapters.hologres.relation import HologresRelation
from dbt.adapters.hologres.relation_configs import (
    HologresIndexConfig,
    MAX_CHARACTERS_IN_IDENTIFIER,
)
from dbt_common.exceptions import DbtRuntimeError


class TestLocalDateEdgeCases:
    """Test LocalDate edge cases and boundary conditions."""

    def test_leap_year_february_29(self):
        """Test February 29 in a leap year."""
        ld = LocalDate("2024-02-29")
        assert ld.year == 2024
        assert ld.month == 2
        assert ld.day == 29

    def test_non_leap_year_february_28(self):
        """Test February 28 in a non-leap year."""
        ld = LocalDate("2023-02-28")
        assert ld.year == 2023
        assert ld.month == 2
        assert ld.day == 28

    def test_sub_years_from_leap_day(self):
        """Test subtracting years from Feb 29 (leap year)."""
        ld = LocalDate("2024-02-29")
        result = ld.sub_years(1)
        # 2023 is not a leap year, should cap at Feb 28
        assert str(result) == "2023-02-28"

    def test_add_years_to_leap_day(self):
        """Test adding years from Feb 29 to non-leap year."""
        ld = LocalDate("2024-02-29")
        result = ld.add_years(4)
        # 2028 is a leap year
        assert str(result) == "2028-02-29"

    def test_sub_months_day_overflow(self):
        """Test subtracting months with day overflow (31st -> 30-day month)."""
        ld = LocalDate("2024-03-31")
        result = ld.sub_months(1)
        # February doesn't have 31 days
        assert str(result) == "2024-02-29"  # 2024 is leap year

    def test_add_months_day_overflow(self):
        """Test adding months with day overflow (31st -> 30-day month)."""
        ld = LocalDate("2024-01-31")
        result = ld.add_months(1)
        # February doesn't have 31 days
        assert str(result) == "2024-02-29"  # 2024 is leap year

    def test_cross_year_subtraction(self):
        """Test date subtraction crossing year boundary."""
        ld = LocalDate("2024-01-05")
        result = ld.sub_days(10)
        assert str(result) == "2023-12-26"

    def test_cross_year_addition(self):
        """Test date addition crossing year boundary."""
        ld = LocalDate("2024-12-28")
        result = ld.add_days(10)
        assert str(result) == "2025-01-07"

    def test_month_overflow_in_addition(self):
        """Test month overflow in addition (Dec + 2 months)."""
        ld = LocalDate("2024-12-15")
        result = ld.add_months(2)
        assert str(result) == "2025-02-15"

    def test_month_underflow_in_subtraction(self):
        """Test month underflow in subtraction (Jan - 2 months)."""
        ld = LocalDate("2024-01-15")
        result = ld.sub_months(2)
        assert str(result) == "2023-11-15"

    def test_end_of_month_february_leap_year(self):
        """Test end_of_month for February in leap year."""
        ld = LocalDate("2024-02-15")
        result = ld.end_of_month()
        assert str(result) == "2024-02-29"

    def test_end_of_month_february_non_leap_year(self):
        """Test end_of_month for February in non-leap year."""
        ld = LocalDate("2023-02-15")
        result = ld.end_of_month()
        assert str(result) == "2023-02-28"

    def test_end_of_month_30_day_month(self):
        """Test end_of_month for 30-day month."""
        ld = LocalDate("2024-04-15")
        result = ld.end_of_month()
        assert str(result) == "2024-04-30"

    def test_end_of_month_31_day_month(self):
        """Test end_of_month for 31-day month."""
        ld = LocalDate("2024-01-15")
        result = ld.end_of_month()
        assert str(result) == "2024-01-31"

    def test_large_days_addition(self):
        """Test adding large number of days."""
        ld = LocalDate("2024-01-01")
        result = ld.add_days(365)
        assert str(result) == "2024-12-31"  # 2024 is leap year

    def test_large_days_subtraction(self):
        """Test subtracting large number of days."""
        ld = LocalDate("2024-12-31")
        result = ld.sub_days(365)
        assert str(result) == "2024-01-01"

    def test_zero_days_operations(self):
        """Test adding/subtracting zero days."""
        ld = LocalDate("2024-06-15")
        assert ld.add_days(0) == ld
        assert ld.sub_days(0) == ld

    def test_zero_months_operations(self):
        """Test adding/subtracting zero months."""
        ld = LocalDate("2024-06-15")
        assert ld.add_months(0) == ld
        assert ld.sub_months(0) == ld

    def test_zero_years_operations(self):
        """Test adding/subtracting zero years."""
        ld = LocalDate("2024-06-15")
        assert ld.add_years(0) == ld
        assert ld.sub_years(0) == ld


class TestCredentialsEdgeCases:
    """Test HologresCredentials edge cases."""

    def test_empty_password(self):
        """Test credentials with empty password."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="",
            database="test_db",
            schema="public",
        )
        assert creds.password == ""

    def test_special_characters_in_password(self):
        """Test credentials with special characters in password."""
        special_chars = "p@ss!w0rd#$%^&*()_+-=[]{}|;':\",./<>?"
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password=special_chars,
            database="test_db",
            schema="public",
        )
        assert creds.password == special_chars

    def test_port_boundary_minimum(self):
        """Test port at minimum valid value (0)."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            port=0,
        )
        assert creds.port == 0

    def test_port_boundary_maximum(self):
        """Test port at maximum valid value (65535)."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            port=65535,
        )
        assert creds.port == 65535

    def test_empty_schema_defaults_to_empty_string(self):
        """Test that empty schema is set to empty string."""
        # Use from_dict to trigger __pre_deserialize__ which sets default schema
        creds = HologresCredentials.from_dict({
            "host": "test.hologres.aliyuncs.com",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
        })
        assert creds.schema == ""

    def test_special_characters_in_host(self):
        """Test host with special characters (e.g., domain with hyphens)."""
        creds = HologresCredentials(
            host="my-test-instance.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )
        assert "my-test-instance" in creds.host

    def test_user_with_dollar_sign(self):
        """Test user with $ character (common in Aliyun BASIC$user format)."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="BASIC$test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )
        assert creds.user == "BASIC$test_user"

    def test_long_application_name(self):
        """Test custom application name."""
        long_name = "my_very_long_application_name_for_testing_purposes"
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
            application_name=long_name,
        )
        assert creds.application_name == long_name


class TestConfigEdgeCases:
    """Test HologresConfig edge cases."""

    def test_empty_index_list(self):
        """Test config with empty index list."""
        config = HologresConfig(indexes=[])
        assert config.indexes == []

    def test_none_index_list(self):
        """Test config with None index list."""
        config = HologresConfig(indexes=None)
        assert config.indexes is None

    def test_multiple_indexes(self):
        """Test config with multiple indexes."""
        config = HologresConfig(
            indexes=[
                HologresIndexConfig(columns=["col1"], unique=True),
                HologresIndexConfig(columns=["col2", "col3"], unique=False),
            ]
        )
        assert len(config.indexes) == 2

    def test_long_partition_key(self):
        """Test config with very long partition key."""
        long_key = "very_long_partition_key_name_for_testing_edge_cases"
        config = HologresConfig(logical_partition_key=long_key)
        assert config.logical_partition_key == long_key

    def test_partition_key_with_whitespace(self):
        """Test config with partition key containing whitespace."""
        config = HologresConfig(logical_partition_key=" ds ")
        assert config.logical_partition_key == " ds "

    def test_all_properties_set(self):
        """Test config with all properties set."""
        config = HologresConfig(
            orientation="column",
            distribution_key="id",
            clustering_key="created_at",
            event_time_column="updated_at",
            bitmap_columns="status,type",
            dictionary_encoding_columns="category",
            logical_partition_key="ds",
        )
        assert config.orientation == "column"
        assert config.distribution_key == "id"
        assert config.clustering_key == "created_at"
        assert config.event_time_column == "updated_at"
        assert config.bitmap_columns == "status,type"
        assert config.dictionary_encoding_columns == "category"
        assert config.logical_partition_key == "ds"

    def test_empty_string_properties(self):
        """Test config with empty string properties."""
        config = HologresConfig(
            orientation="",
            distribution_key="",
        )
        assert config.orientation == ""
        assert config.distribution_key == ""


class TestRelationEdgeCases:
    """Test HologresRelation edge cases."""

    def test_identifier_at_max_length(self):
        """Test identifier at maximum allowed length."""
        max_name = "a" * MAX_CHARACTERS_IN_IDENTIFIER
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier=max_name,
            type="table",
        )
        assert relation.identifier == max_name

    def test_identifier_exceeds_max_length(self):
        """Test identifier exceeding maximum length raises error."""
        long_name = "a" * (MAX_CHARACTERS_IN_IDENTIFIER + 1)
        with pytest.raises(DbtRuntimeError) as exc_info:
            HologresRelation.create(
                database="test_db",
                schema="test_schema",
                identifier=long_name,
                type="table",
            )
        assert "longer than" in str(exc_info.value)

    def test_identifier_with_underscore(self):
        """Test identifier with underscore."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table_name",
            type="table",
        )
        assert relation.identifier == "test_table_name"

    def test_identifier_with_numbers(self):
        """Test identifier with numbers."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="table_123_test",
            type="table",
        )
        assert relation.identifier == "table_123_test"

    def test_identifier_none_ignored(self):
        """Test None identifier doesn't trigger length check."""
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier=None,
        )
        assert relation.identifier is None

    def test_type_none_ignored(self):
        """Test None type doesn't trigger length check."""
        long_name = "a" * 1000
        relation = HologresRelation.create(
            database="test_db",
            schema="test_schema",
            identifier=long_name,
            type=None,
        )
        assert relation.identifier == long_name


class TestIndexConfigEdgeCases:
    """Test HologresIndexConfig edge cases."""

    def test_single_column_index(self):
        """Test index with single column."""
        config = HologresIndexConfig(columns=["col1"])
        assert config.columns == ["col1"]

    def test_multiple_column_index(self):
        """Test index with multiple columns."""
        config = HologresIndexConfig(columns=["col1", "col2", "col3"])
        assert len(config.columns) == 3

    def test_unique_index(self):
        """Test unique index configuration."""
        config = HologresIndexConfig(columns=["id"], unique=True)
        assert config.unique is True

    def test_non_unique_index(self):
        """Test non-unique index configuration."""
        config = HologresIndexConfig(columns=["name"], unique=False)
        assert config.unique is False

    def test_index_default_unique(self):
        """Test index default unique value."""
        config = HologresIndexConfig(columns=["col1"])
        assert config.unique is False

    def test_index_parse_valid(self):
        """Test parsing valid index config."""
        result = HologresIndexConfig.parse({"columns": ["col1", "col2"], "unique": True})
        assert result.columns == ["col1", "col2"]
        assert result.unique is True

    def test_index_parse_none(self):
        """Test parsing None returns None."""
        result = HologresIndexConfig.parse(None)
        assert result is None


class TestLocalDateStringEncoding:
    """Test LocalDate string parsing edge cases."""

    def test_iso_format_with_time(self):
        """Test parsing ISO format with time component."""
        ld = LocalDate("2024-06-15T10:30:00")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15

    def test_datetime_string_with_space(self):
        """Test parsing datetime string with space separator."""
        ld = LocalDate("2024-06-15 10:30:00")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15

    def test_slash_format(self):
        """Test parsing slash format date."""
        ld = LocalDate("2024/06/15")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15

    def test_compact_format(self):
        """Test parsing compact format date."""
        ld = LocalDate("20240615")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15

    def test_invalid_format_raises_error(self):
        """Test invalid format raises ValueError."""
        with pytest.raises(ValueError):
            LocalDate("not-a-date")

    def test_invalid_format_numeric_raises_error(self):
        """Test invalid numeric format raises ValueError."""
        with pytest.raises(ValueError):
            LocalDate("2024/13/45")  # Invalid month/day

    def test_whitespace_in_date_string(self):
        """Test date string with leading/trailing whitespace."""
        ld = LocalDate("  2024-06-15  ")
        assert ld.year == 2024
        assert ld.month == 6
        assert ld.day == 15


class TestQuarterBoundaries:
    """Test quarter boundary calculations."""

    def test_q1_start(self):
        """Test Q1 start (January 1)."""
        ld = LocalDate("2024-02-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-01-01"

    def test_q2_start(self):
        """Test Q2 start (April 1)."""
        ld = LocalDate("2024-05-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-04-01"

    def test_q3_start(self):
        """Test Q3 start (July 1)."""
        ld = LocalDate("2024-08-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-07-01"

    def test_q4_start(self):
        """Test Q4 start (October 1)."""
        ld = LocalDate("2024-11-15")
        result = ld.start_of_quarter()
        assert str(result) == "2024-10-01"

    def test_q1_end(self):
        """Test Q1 end (March 31)."""
        ld = LocalDate("2024-02-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-03-31"

    def test_q2_end(self):
        """Test Q2 end (June 30)."""
        ld = LocalDate("2024-05-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-06-30"

    def test_q3_end(self):
        """Test Q3 end (September 30)."""
        ld = LocalDate("2024-08-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-09-30"

    def test_q4_end(self):
        """Test Q4 end (December 31)."""
        ld = LocalDate("2024-11-15")
        result = ld.end_of_quarter()
        assert str(result) == "2024-12-31"