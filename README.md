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
