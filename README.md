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
pip install dbt-hologres
```

#### Install from Source

For development or to get the latest features, you can install directly from the source code:

```bash
# Clone the repository
git clone https://github.com/your-org/dbt-hologres.git
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

### Key Features

- **Full PostgreSQL Compatibility**: Leverage familiar PostgreSQL syntax and features
- **Psycopg3 Driver**: Uses the modern Psycopg 3 library for better performance
- **Dynamic Tables**: Support for Hologres Dynamic Tables (materialized views with auto-refresh)
- **Incremental Models**: Multiple strategies including append, delete+insert, merge, and microbatch
- **Constraints**: Full support for primary keys, foreign keys, unique constraints, and more

### Hologres-Specific Features

#### Dynamic Tables

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

### Connection Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| host | Yes | - | Hologres instance hostname |
| port | No | 80 | Port number |
| user | Yes | - | Username (case-sensitive) |
| password | Yes | - | Password (case-sensitive) |
| database | Yes | - | Database name |
| schema | Yes | "" | Default schema (use empty string "" if not needed) |
| threads | No | 1 | Number of threads for parallel execution |
| connect_timeout | No | 10 | Connection timeout in seconds |
| sslmode | No | disable | SSL mode (disabled by default) |
| application_name | No | dbt_hologres_{version} | Application identifier |
| retries | No | 1 | Number of connection retries |

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

### Supported dbt Versions

- dbt-core >= 1.8.0
- Python >= 3.11

## Running Tests

This project includes both unit tests and integration tests.

### Unit Tests

Unit tests use mocked database connections and can be run without a Hologres instance:

```bash
# Run all unit tests
pytest tests/unit/

# Run a specific test file
pytest tests/unit/test_connection.py

# Run with verbose output
pytest tests/unit/ -v
```

### Integration Tests

Integration tests require an actual Hologres database connection and perform real database operations including creating, updating, and dropping tables.

#### Prerequisites

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

#### Running Integration Tests

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

#### Integration Test Structure

The integration test suite includes:

- **test_table_operations.py**: Tests for table creation, updates, deletion, and incremental models
- **test_view_operations.py**: Tests for view creation, dependencies, and conversions
- **test_hologres_features.py**: Tests for Hologres-specific features like indexes, dynamic tables, and partitioning

Each test uses an isolated schema to ensure tests don't interfere with each other. Test schemas are automatically cleaned up after each test run.

#### Test Isolation

Integration tests create unique schemas for each test to ensure isolation:
- Each test gets a unique schema name (e.g., `test_a1b2c3d4e5f6g7h8i9j0`)
- Tests clean up their schemas automatically after completion
- Failed tests still attempt cleanup

### Resources

- [dbt Documentation](https://docs.getdbt.com)
- [Hologres Documentation](https://help.aliyun.com/zh/hologres/)
- [Hologres Dynamic Table Guide](https://help.aliyun.com/zh/hologres/user-guide/introduction-to-dynamic-table)

### License

Apache License 2.0

### Support

For issues and questions:
- [GitHub Issues](https://github.com/dbt-labs/dbt-adapters/issues)
- [dbt Community Slack](https://www.getdbt.com/community/)
