# dbt-hologres

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-hologres

`dbt-hologres` enables dbt to work with Alibaba Cloud Hologres, a real-time data warehouse compatible with PostgreSQL.

For more information on using dbt with Hologres, consult the [dbt documentation](https://docs.getdbt.com).

## Getting started

### Installation

#### Install from PyPI

```bash
pip install dbt-alibaba-cloud-hologres
```

#### Install from Source

For development or to get the latest features, you can install directly from the source code:

```bash
# Clone the repository
git clone https://github.com/aliyun/dbt-hologres.git
cd dbt-hologres

# Install in editable mode
pip install --force-reinstall -e .
```

This allows you to:

- Modify the adapter code and see changes immediately
- Contribute to the project development
- Test unreleased features

### Configuration

Configure your `profiles.yml` file:

```yaml
hologres_project:
  target: dev
  outputs:
    dev:
      type: hologres
      host: hgxxx-xx-xxx-xx-xxx.hologres.aliyuncs.com
      port: 80
      user: BASIC$your_username
      password: your_password
      database: your_database
      schema: ""  # Use empty string if no default schema needed
      threads: 4
      connect_timeout: 10
      sslmode: disable
```

## Product Features

### Core Adapter Features

| Feature | Description |
|---------|-------------|
| **HologresAdapter** | Core adapter with PostgreSQL-compatible syntax support |
| **Psycopg3 Driver** | Uses modern Psycopg 3 library for better performance |
| **Incremental Strategies** | Multiple strategies: `append`, `delete+insert`, `merge`, `microbatch` |
| **Constraints** | Full support for `primary key`, `not null`, `unique`, `foreign key` constraints |
| **Catalog by Relation** | Enabled for better metadata management |

### Table Properties Configuration

Hologres-specific table properties can be configured in your model:

| Property | Description |
|----------|-------------|
| `orientation` | Storage direction: `column` or `row` |
| `distribution_key` | Distribution key for data sharding |
| `clustering_key` | Clustering key for query optimization |
| `event_time_column` / `segment_key` | Event time column for time-series data |
| `bitmap_columns` | Bitmap index columns |
| `dictionary_encoding_columns` | Dictionary encoding columns |

Example configuration:

```yaml
models:
  my_model:
    materialized: table
    orientation: column
    distribution_key: user_id
    clustering_key: created_at
    event_time_column: created_at
    bitmap_columns: status,type
    dictionary_encoding_columns: category
```

### Dynamic Tables

Dynamic Tables are Hologres's implementation of materialized views with automatic refresh:

```yaml
models:
  my_model:
    materialized: dynamic_table
    freshness: "30 minutes"
    auto_refresh_mode: auto
    computing_resource: serverless
```

Supported configurations:

- `freshness`: Data freshness requirement (e.g., "30 minutes", "1 hours")
- `auto_refresh_mode`: `auto`, `incremental`, or `full`
- `computing_resource`: `serverless`, `local`, or warehouse name
- Logical partitioning support for time-series data

### Logical Partition Tables

Logical Partition Tables enable efficient data management and query optimization:

```yaml
models:
  my_model:
    materialized: table
    logical_partition_key: 'ds'  # Single partition key
    # or for multiple keys:
    # logical_partition_key: 'order_year, order_month'
```

Supported configurations:

- `logical_partition_key`: Partition column(s), supports 1-2 columns separated by comma
- Supported types: INT, TEXT, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ
- Partition keys are automatically set to NOT NULL
- Works with table properties like `orientation`, `distribution_key`, etc.

### LocalDate Date Utilities

The adapter provides a powerful `LocalDate` class for date manipulation in Jinja2 templates:

#### Basic Usage

```jinja2
{%- set ds = adapter.parse_date('2024-03-15') -%}

{# Date arithmetic #}
{{ ds.sub_days(7) }}        {# 2024-03-08 #}
{{ ds.add_months(2) }}      {# 2024-05-15 #}

{# Period boundaries #}
{{ ds.start_of_month() }}   {# 2024-03-01 #}
{{ ds.end_of_quarter() }}   {# 2024-03-31 #}
```

#### Chained Operations

```jinja2
{%- set ds = adapter.parse_date('2024-03-15') -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{{ start_date }}  {# 2024-01-01 #}
```

#### Available Methods

| Category | Methods |
|----------|---------|
| **Arithmetic** | `add_days()`, `sub_days()`, `add_months()`, `sub_months()`, `add_years()`, `sub_years()` |
| **Period Boundaries** | `start_of_month()`, `end_of_month()`, `start_of_quarter()`, `end_of_quarter()`, `start_of_year()`, `end_of_year()`, `start_of_week()`, `end_of_week()` |
| **Formatting** | `format()`, `str()` |
| **Comparison** | `is_before()`, `is_after()`, `is_equal()`, `days_between()` |
| **Properties** | `year`, `month`, `day`, `quarter`, `day_of_year` |

#### Helper Functions

```jinja2
{# Get today's date #}
{%- set today = adapter.today() -%}

{# Parse date from string #}
{%- set ds = adapter.parse_date('2024-06-15') -%}
{%- set ds = adapter.parse_date('2024/06/15') -%}   {# Slash format #}
{%- set ds = adapter.parse_date('20240615') -%}     {# Compact format #}
```

### SQL Macros

The adapter provides various SQL macros for Hologres-specific operations:

#### CTAS with Properties

```jinja2
{%- set with_properties = [
    "orientation = 'column'",
    "distribution_key = 'id'"
] -%}
create table {{ relation }}
with (
  {{ with_properties | join(',\n  ') }}
)
as (
  {{ compiled_code }}
);
```

#### Logical Partition Table DDL

```jinja2
create table {{ relation }} (
  {{ column_definitions }}
)
logical partition by list ({{ partition_columns }});
```

#### Date Utility Macros

```jinja2
{# Date range generation #}
{%- set start = adapter.parse_date('2024-01-01') -%}
{%- set end = adapter.parse_date('2024-01-05') -%}
{%- set days = start.days_between(end) -%}

{# Format date #}
{{ ds.format('%Y%m%d') }}
```

### Connection Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `host` | Yes | - | Hologres instance hostname |
| `port` | No | 80 | Port number |
| `user` | Yes | - | Username (case-sensitive) |
| `password` | Yes | - | Password (case-sensitive) |
| `database` | Yes | - | Database name |
| `schema` | Yes | "" | Default schema (use empty string "" if not needed) |
| `threads` | No | 1 | Number of threads for parallel execution |
| `connect_timeout` | No | 10 | Connection timeout in seconds |
| `sslmode` | No | disable | SSL mode (disabled by default) |
| `application_name` | No | dbt_hologres_{version} | Application identifier |
| `retries` | No | 1 | Number of connection retries |

### Testing Your Connection

Run `dbt debug` to verify your connection:

```bash
dbt debug
```

### Example Project Structure

```
my_hologres_project/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   ├── marts/
│   │   └── fct_orders.sql
│   └── schema.yml
└── tests/
    └── assert_positive_order_total.sql
```

### Important Notes

1. **Case Sensitivity**: Hologres usernames and passwords are case-sensitive
2. **Default Port**: Default port is 80 (not 5432 like PostgreSQL)
3. **SSL Mode**: SSL is disabled by default for Hologres connections
4. **Psycopg3**: This adapter uses Psycopg 3, which has API differences from Psycopg 2
5. **Model Name Restrictions**: Model names must not exceed 27 characters and are case-insensitive (converted to lowercase)

### Supported dbt Versions

- dbt-core >= 1.8.0
- Python >= 3.10

## Unit Tests

This project includes comprehensive unit tests with mocked database connections.

### Test Coverage Overview

| Test File | Test Classes | Test Methods | Coverage Area |
|-----------|--------------|--------------|---------------|
| test_adapter.py | 5 | 29 | Adapter core functionality and configuration |
| test_connection.py | 5 | 28 | Connection management and credentials |
| test_relation.py | 3 | 18 | Relation objects and index management |
| test_column.py | 1 | 8 | Column handling and data types |
| test_local_date.py | 12 | 64 | LocalDate date utilities |
| test_index_config.py | 2 | 18 | Index configuration |
| test_dynamic_table_config.py | 2 | 16 | Dynamic table configuration |
| test_sql_macros.py | 6 | 27 | SQL macro rendering |
| test_logical_partition.py | 5 | 22 | Logical partition tables |
| test_date_utils_macros.py | 9 | 33 | Date utility macros |
| test_exception_handling.py | 7 | 23 | Exception handling |
| test_edge_cases.py | 7 | 64 | Edge cases and boundary conditions |
| **Total** | **61** | **~350** | |

### Test Class Descriptions

#### test_adapter.py
- `TestHologresAdapter`: Core adapter functionality, incremental strategies, timestamp operations
- `TestHologresConfig`: Table property configuration (orientation, distribution_key, etc.)
- `TestHologresAdapterParseDate`: Date parsing functionality
- `TestHologresAdapterToday`: Current date retrieval
- `TestHologresAdapterTimestampAdd`: Timestamp interval SQL generation

#### test_connection.py
- `TestHologresCredentials`: Credential validation and defaults
- `TestHologresConnectionManager`: Connection lifecycle management
- `TestHologresConnectionManagerOpen`: Connection opening behavior
- `TestHologresCredentialsValidation`: Credential boundary validation
- `TestGetResponse`: SQL response parsing

#### test_relation.py
- `TestHologresRelation`: Relation creation and properties
- `TestGetIndexConfigChanges`: Index configuration change detection
- `TestDynamicTableConfigChanges`: Dynamic table configuration changes

#### test_local_date.py
- `TestLocalDateCreation`: Date object creation from various inputs
- `TestLocalDateSubtraction`: Date subtraction operations
- `TestLocalDateAddition`: Date addition operations
- `TestLocalDatePeriodBoundaries`: Month, quarter, year boundaries
- `TestLocalDateChaining`: Method chaining support
- `TestLocalDateProperties`: Year, month, day, quarter properties
- `TestLocalDateFormatting`: Date format string generation
- `TestLocalDateComparison`: Date comparison methods
- `TestParseDateFunction`: parse_date helper function
- `TestTodayFunction`: today() helper function
- `TestLocalDateWeekMethods`: Week boundary methods
- `TestLocalDateDayOfYear`: Day of year property

#### test_sql_macros.py
- `TestAdaptersMacros`: CTAS and DDL statement rendering
- `TestRelationOperations`: Drop, truncate, rename operations
- `TestSchemaOperations`: Schema creation and deletion
- `TestViewOperations`: View creation and management
- `TestInsertOperations`: Insert statement generation
- `TestTimestampOperations`: Timestamp interval operations

## Running Tests

### Using Hatch (Recommended)

[Hatch](https://hatch.pypa.io/) is the recommended way to run tests:

```bash
# Install hatch
pip install hatch

# Run all unit tests
hatch -e cd run unit-tests

# Run a specific test file
hatch -e cd run unit-tests tests/unit/test_adapter.py

# Run a specific test class
hatch -e cd run unit-tests tests/unit/test_adapter.py::TestHologresAdapter

# Run a specific test method
hatch -e cd run unit-tests tests/unit/test_adapter.py::TestHologresAdapter::test_date_function
```

### Using pytest Directly

You can also run tests directly with pytest:

```bash
# Install dependencies
pip install -e .
pip install pytest pytest-mock pytest-xdist freezegun

# Run all unit tests
python -m pytest tests/unit -v

# Run with parallel execution
python -m pytest tests/unit -v -n auto

# Run a specific test file
python -m pytest tests/unit/test_connection.py -v

# Run with coverage report
python -m pytest tests/unit --cov=src/dbt/adapters/hologres --cov-report=term-missing
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `unit` | Unit tests (default for tests/unit/) |
| `integration` | Integration tests (requires database connection) |
| `slow` | Slow-running tests |
| `smoke` | Basic smoke tests |

## Integration Tests

Integration tests require an actual Hologres database connection and perform real database operations including creating, updating, and dropping tables.

### Prerequisites

Before running integration tests, configure your Hologres connection using one of the following methods:

**Method 1: Using test.env file (Recommended)**

1. Copy the example environment file:

```bash
cp test.env.example test.env
```

2. Edit `test.env` and fill in your actual Hologres connection details:

```bash
# Hologres instance configuration
DBT_HOLOGRES_HOST=your_hologres_instance.hologres.aliyuncs.com
DBT_HOLOGRES_PORT=80
DBT_HOLOGRES_USER='BASIC$your_username'
DBT_HOLOGRES_PASSWORD='your_password'
DBT_HOLOGRES_DATABASE='your_database'
DBT_HOLOGRES_SCHEMA='test_schema'

# Enable integration tests
DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true
```

3. Load the environment variables before running tests:

```bash
# Load environment variables from test.env
export $(cat test.env | grep -v '^#' | xargs)

# Run integration tests
pytest tests/integration/
```

**Method 2: Setting environment variables directly**

```bash
export DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true
export DBT_HOLOGRES_HOST=your_hologres_instance.hologres.aliyuncs.com
export DBT_HOLOGRES_PORT=80
export DBT_HOLOGRES_USER=your_username
export DBT_HOLOGRES_PASSWORD=your_password
export DBT_HOLOGRES_DATABASE=your_database
export DBT_HOLOGRES_SCHEMA=test_schema  # Optional, defaults to 'test_schema'
```

### Running Integration Tests

```bash
# Run all integration tests
pytest tests/integration/

# Run specific integration test
pytest tests/integration/test_table_operations.py

# Run with verbose output
pytest tests/integration/ -v

# Run only table operation tests
pytest tests/integration/test_table_operations.py -v

# Run only view operation tests
pytest tests/integration/test_view_operations.py -v

# Run only Hologres-specific feature tests
pytest tests/integration/test_hologres_features.py -v
```

### Integration Test Structure

The integration test suite includes:

- **test_table_operations.py**: Tests for table creation, updates, deletion, and incremental models
- **test_view_operations.py**: Tests for view creation, dependencies, and conversions
- **test_hologres_features.py**: Tests for Hologres-specific features like indexes, dynamic tables, and partitioning

Each test uses an isolated schema to ensure tests don't interfere with each other. Test schemas are automatically cleaned up after each test run.

### Test Isolation

Integration tests create unique schemas for each test to ensure isolation:

- Each test gets a unique schema name (e.g., `test_a1b2c3d4e5f6g7h8i9j0`)
- Tests clean up their schemas automatically after completion
- Failed tests still attempt cleanup

## Resources

- [dbt Documentation](https://docs.getdbt.com)
- [Hologres Documentation](https://help.aliyun.com/zh/hologres/)
- [Hologres Dynamic Table Guide](https://help.aliyun.com/zh/hologres/user-guide/introduction-to-dynamic-table)

## License

Apache License 2.0

## Support

For issues and questions:

- [GitHub Issues](https://github.com/dbt-labs/dbt-adapters/issues)
- [dbt Community Slack](https://www.getdbt.com/community/)