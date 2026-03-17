# dbt-hologres Example Project

This directory contains example dbt models demonstrating the key features of the dbt-hologres adapter.

## Setup

1. Configure your `profiles.yml`:

```yaml
hologres_example:
  target: dev
  outputs:
    dev:
      type: hologres
      host: hgxxx-xx-xxx-xx-xxx.hologres.aliyuncs.com
      port: 80
      user: BASIC$dbt_user
      password: your_password
      database: from_dbt
      schema: public
      threads: 4
```

2. Run the example:

```bash
dbt debug  # Verify connection
dbt seed   # Load seed data from CSV files
dbt run    # Run all models
dbt test   # Run tests
dbt docs generate  # Generate documentation
dbt docs serve    # Serve documentation at http://localhost:8080
```

## Example Models

### 1. Simple Table Model
`models/staging/stg_orders.sql` - Basic table materialization

### 2. View Model
`models/marts/orders_summary.sql` - View materialization

### 3. Incremental Model
`models/marts/orders_incremental.sql` - Incremental updates with merge strategy

### 4. Dynamic Table Model
`models/marts/orders_dynamic_table.sql` - Hologres Dynamic Table with auto-refresh

### 5. Table with Properties
`models/marts/table_with_properties.sql` - Table with Hologres-specific properties (orientation, distribution_key, etc.)

### 6. Logical Partition Table (Single Key)
`models/marts/logical_partition_table.sql` - Logical partition table with single partition column

- Partition by date column (`ds`) for efficient data management
- Supported partition types: INT, TEXT, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ
- Partition keys are automatically set to NOT NULL

```yaml
config:
  logical_partition_key: 'ds'
```

### 7. Logical Partition Table (Multi Keys)
`models/marts/lpt_multi_keys.sql` - Logical partition table with 2 partition columns

- Supports up to 2 partition columns (e.g., year + month)
- Useful for hierarchical time-based partitioning
- Query optimization: filtering by partition keys scans only relevant partitions

```yaml
config:
  logical_partition_key: 'order_year, order_month'
```

## Seed Data

### Sales Targets
`seeds/salestargets.csv` - Sample sales target data for stores

Seeds are CSV files in your `seeds/` directory that dbt can load into your data warehouse:

```bash
# Load all seed files
dbt seed

# Load specific seed file
dbt seed --select salestargets

# Full refresh (drop and recreate)
dbt seed --full-refresh
```

Seeds are useful for:
- Loading reference data (e.g., country codes, status mappings)
- Loading test data for development
- Loading small lookup tables that change infrequently

**Note**: Seeds are best for small datasets (< 1MB). For larger datasets, use external data loading tools.

## Model Configurations

See `dbt_project.yml` for model-specific configurations including:
- Materialization strategies
- Dynamic Table settings
- Incremental strategies
- Schema configurations
