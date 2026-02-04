# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0]

### Features

- **Logical Partition Tables**: Added support for Hologres logical partition tables
  - New `logical_partition_key` configuration option supporting 1-2 partition columns
  - Enhanced table materialization with logical partition table creation workflow (CTAS for schema inference, then DDL creation with data insertion)
  - Partition keys are automatically set to NOT NULL constraint
  - Supported types: INT, TEXT, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ
  - Works with other table properties: `orientation`, `distribution_key`, `clustering_key`, etc.
  - Added example models: `logical_partition_table.sql` (single key) and `lpt_multi_keys.sql` (multi keys)
- **LocalDate Date Utility**: Added `LocalDate` class and `parse_date()` function for chainable date operations in Jinja2 templates
  - Support for date parsing from multiple formats (YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD)
  - Subtraction methods: `sub_days()`, `sub_months()`, `sub_years()`
  - Addition methods: `add_days()`, `add_months()`, `add_years()`
  - Period boundary methods: `start_of_month()`, `end_of_month()`, `start_of_quarter()`, `end_of_quarter()`, `start_of_year()`, `end_of_year()`
  - Property accessors: `year`, `month`, `day`, `quarter`
  - Custom formatting with `format()` method
- **Date Utility Macros**: Added Jinja2 macros in `date_utils.sql`
  - `parse_date()`: Parse date string into LocalDate
  - `today()`: Get today's date as LocalDate
  - `ds()`: Get execution date from `EXECUTION_DATE` variable or environment
  - `format_date()`: Format LocalDate to string
  - `date_range()`: Generate list of dates between two dates
- **Demo Model**: Added `date_utils_demo.sql` example showing LocalDate usage

### Example Usage

**Logical Partition Table:**

For detailed use cases, please refer to the [reference document](examples/models/marts/logical_partition_table.sql).

**LocalDate in Jinja2:**
```jinja2
{%- set ds = ds() -%}
{%- set start_quarter = ds.sub_months(2).start_of_quarter() -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{%- set end_date = ds.start_of_month() -%}
{%- set start_date_last = start_date.sub_months(12) -%}
{%- set end_date_last = end_date.sub_months(12) -%}
```
For detailed use cases, please refer to the [reference document](examples/models/marts/date_utils_demo.sql).

## [1.0.0]

### Features

- Initial release of dbt-hologres adapter
- Support for Alibaba Cloud Hologres data warehouse
- Uses Psycopg 3 for database connectivity
- PostgreSQL-compatible SQL syntax
- Full support for standard dbt materializations: table, view, incremental
- Support for Hologres Dynamic Tables with auto-refresh
- Multiple incremental strategies: append, delete+insert, merge, microbatch
- Some constraint support: primary keys, not null, check
- **GUC Parameters via sql_header**: Support setting session-level GUC (Grand Unified Configuration) parameters, etc:
  - Set timezone: `sql_header="SET TIME ZONE 'UTC';"`
  - Use serverless computing: `sql_header="SET hg_computing_resource = 'serverless';"`
- SSL disabled by default for Hologres connections
- Case-sensitive username and password authentication
- Custom application_name with version tracking
- Comprehensive test suite
- Example project with sample models
- Complete documentation

### Breaking Changes

- None (initial release)

### Under the Hood

- Built on dbt-adapters framework v1.19.0+
- Requires Python 3.10+
- Requires dbt-core 1.8.0+
- Uses Psycopg 3 instead of Psycopg 2
