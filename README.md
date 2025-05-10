# DuckQuery.jl

A Julia library that provides a clean, flexible interface for executing SQL queries against various data sources using DuckDB as the backend engine.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Usage Scenarios](#usage-scenarios)
  - [Database File Operations](#database-file-operations)
  - [DataFrame as Data Source](#dataframe-as-data-source)
  - [Multiple DataFrames](#multiple-dataframes)
  - [Mixed Data Sources](#mixed-data-sources)
  - [Connection Management](#connection-management)
  - [Data Processing Pipelines](#data-processing-pipelines)
  - [Advanced SQL Features](#advanced-sql-features)
  - [Error Handling](#error-handling)
  - [Performance Optimization](#performance-optimization)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

## Overview

DuckQuery.jl combines the power of DuckDB's SQL engine with Julia's DataFrames, providing a unified interface for working with tabular data. It enables you to query data from various sources using familiar SQL syntax while maintaining a clean, intuitive API.

## Key Features

- **Multiple Source Types**: Query database files, in-memory DataFrames, or combinations of both
- **Connection Management**: Automatic management or control via do-blocks
- **Data Processing**: Pre- and post-processing hooks for query and result transformation
- **Performance Tuning**: Configuration options for memory limits, threading, and more
- **Extension Support**: Easily use DuckDB extensions for specialized functionality
- **Error Handling**: Flexible error handling strategies
- **Performance Profiling**: Built-in timing for query optimization

## Installation

```julia
using Pkg
Pkg.add(url="https://github.com/yourusername/DuckQuery.jl")
```

To use with the latest Julia version:

```julia
]add DuckQuery
```

## Basic Usage

Here's a quick overview of how to use DuckQuery.jl:

```julia
using DuckQuery
using DataFrames

# Query a database file
results = querydf("data.db", "SELECT * FROM table LIMIT 10")

# Query an in-memory DataFrame
df = DataFrame(id = 1:3, name = ["Alice", "Bob", "Charlie"])
results = querydf(df, "SELECT * FROM df WHERE id > 1")

# Execute a query with verbosity and profiling
results = querydf(
    "data.db",
    "SELECT * FROM table WHERE value > 100",
    verbose = true,
    profile = true
)
```

## Usage Scenarios

### Database File Operations

#### Working with Database Files

```julia
using DuckQuery
using DataFrames

# Query an existing database file
results = querydf(
    "customers.db",
    "SELECT * FROM customers WHERE region = 'East' LIMIT 20"
)

# Create a new database and tables
querydf(
    "new_database.db",
    [
        "CREATE TABLE customers (id INTEGER, name TEXT, region TEXT)",
        "INSERT INTO customers VALUES (1, 'Alice', 'East'), (2, 'Bob', 'West')",
        "CREATE INDEX idx_region ON customers(region)"
    ],
    verbose = true
)

# Query with configuration settings
results = querydf(
    "large_data.db",
    "SELECT * FROM transactions WHERE amount > 1000",
    init_config = Dict{Symbol, Any}(
        :memory_limit => "8GB",
        :threads => 8
    )
)
```

#### Importing and Exporting Data

```julia
# Load data from external files
results = querydf(
    ":memory:",
    "SELECT * FROM read_csv_auto('data.csv') WHERE value > 100"
)

# Export results to files
export_df = querydf(
    "database.db",
    [
        "COPY (SELECT * FROM customers WHERE signup_date > '2023-01-01') TO 'new_customers.csv'",
        "SELECT * FROM customers WHERE signup_date > '2023-01-01'"
    ]
)

# Work with Parquet files
results = querydf(
    ":memory:",
    "SELECT * FROM parquet_scan('large_dataset.parquet') LIMIT 1000",
    init_queries = ["LOAD parquet"]
)
```

### DataFrame as Data Source

#### Basic DataFrame Queries

```julia
using DuckQuery
using DataFrames

# Create a sample DataFrame
df = DataFrame(
    id = 1:5,
    name = ["Alice", "Bob", "Charlie", "David", "Eve"],
    age = [25, 32, 45, 28, 36],
    department = ["Sales", "IT", "HR", "Sales", "IT"]
)

# Simple SELECT queries
results = querydf(df, "SELECT * FROM df WHERE age > 30")

# Aggregation and grouping
results = querydf(
    df,
    """
    SELECT
        department,
        COUNT(*) as employee_count,
        AVG(age) as avg_age
    FROM df
    GROUP BY department
    ORDER BY employee_count DESC
    """
)

# Using SQL functions with DataFrame data
results = querydf(
    df,
    """
    SELECT
        id,
        name,
        age,
        CASE
            WHEN age < 30 THEN 'Junior'
            WHEN age < 40 THEN 'Mid-level'
            ELSE 'Senior'
        END as seniority
    FROM df
    ORDER BY age
    """
)
```

#### DataFrame Transformation

```julia
# Use SQL to reshape/pivot data
sales_data = DataFrame(
    date = repeat(["2023-01-01", "2023-01-02", "2023-01-03"], inner=3),
    region = repeat(["East", "West", "Central"], 3),
    amount = [100, 150, 200, 120, 180, 210, 130, 190, 220]
)

# Pivot table using SQL
pivot_results = querydf(
    sales_data,
    """
    SELECT
        date,
        SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) as East,
        SUM(CASE WHEN region = 'West' THEN amount ELSE 0 END) as West,
        SUM(CASE WHEN region = 'Central' THEN amount ELSE 0 END) as Central
    FROM df
    GROUP BY date
    ORDER BY date
    """
)
```

### Multiple DataFrames

#### Joining DataFrames

```julia
using DuckQuery
using DataFrames

# Create sample DataFrames
customers = DataFrame(
    id = 1:5,
    name = ["Alice", "Bob", "Charlie", "David", "Eve"],
    city = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
)

orders = DataFrame(
    id = 101:107,
    customer_id = [1, 2, 1, 3, 2, 4, 5],
    product = ["Widget A", "Widget B", "Widget C", "Widget B", "Widget A", "Widget C", "Widget A"],
    amount = [150.0, 200.0, 120.0, 300.0, 250.0, 175.0, 220.0]
)

# Join DataFrames and perform analysis
results = querydf(
    Dict("customers" => customers, "orders" => orders),
    """
    SELECT
        c.name,
        c.city,
        COUNT(o.id) as order_count,
        SUM(o.amount) as total_spent,
        AVG(o.amount) as avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    GROUP BY c.name, c.city
    ORDER BY total_spent DESC
    """
)

# Multi-table analysis
products = DataFrame(
    name = ["Widget A", "Widget B", "Widget C"],
    category = ["Standard", "Premium", "Standard"],
    cost = [80.0, 120.0, 90.0]
)

# Profitability analysis across multiple tables
profitability = querydf(
    Dict("customers" => customers, "orders" => orders, "products" => products),
    """
    SELECT
        p.category,
        COUNT(o.id) as sales_count,
        SUM(o.amount) as revenue,
        SUM(o.amount - p.cost) as profit,
        SUM(o.amount - p.cost) / SUM(o.amount) * 100 as profit_margin
    FROM orders o
    JOIN products p ON o.product = p.name
    GROUP BY p.category
    ORDER BY profit_margin DESC
    """
)
```

### Mixed Data Sources

#### Combining Database Files and DataFrames

```julia
using DuckQuery
using DataFrames
using Dates

# Create a sample DataFrame with new customer data
new_customers = DataFrame(
    id = 101:105,
    name = ["Frank", "Grace", "Heidi", "Ivan", "Julia"],
    signup_date = fill(today(), 5)
)

# Query combining database file and DataFrames
results = querydf(
    Dict(
        "existing" => "customers.db",  # Database file
        "new_data" => new_customers   # DataFrame
    ),
    """
    SELECT
        'Existing' as source,
        name,
        signup_date
    FROM existing.customers
    WHERE signup_date > '2023-01-01'

    UNION ALL

    SELECT
        'New' as source,
        name,
        signup_date
    FROM new_data

    ORDER BY signup_date DESC, name
    """
)

# Using database file for lookup tables with in-memory data
transactions = DataFrame(
    id = 1:100,
    product_code = rand(["A001", "B002", "C003", "D004", "E005"], 100),
    amount = rand(10.0:500.0, 100)
)

# Use product catalog in DB with in-memory transaction data
enriched_data = querydf(
    Dict(
        "transactions" => transactions,
        "products" => "product_catalog.db"
    ),
    """
    SELECT
        t.id as transaction_id,
        t.product_code,
        p.name as product_name,
        p.category,
        t.amount
    FROM transactions t
    JOIN products.catalog p ON t.product_code = p.code
    ORDER BY t.id
    """
)
```

### Connection Management

#### Using Do-Blocks for Connection Control

```julia
using DuckQuery
using DataFrames

# Create a sample DataFrame
data = DataFrame(
    id = 1:1000,
    value = rand(1000),
    category = rand(["A", "B", "C"], 1000)
)

# Using do-block to control the connection
results = querydf(
    ":memory:",
    "SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM data_table GROUP BY category"
) do conn
    # Register the DataFrame
    DuckQuery.register_dataframe(conn, "data_temp", data)

    # Create a filtered view for analysis
    DuckDB.execute(conn, """
        CREATE TEMP VIEW data_table AS
        SELECT * FROM data_temp
        WHERE value > 0.3
    """)

    # Could perform more operations on the connection here
    # The query in the outer function will be executed after this block
end

# Performing multiple operations with database file connection
querydf(
    "analytics.db",
    [
        "CREATE TABLE IF NOT EXISTS summary AS SELECT * FROM aggregated_data",
        "SELECT * FROM summary ORDER BY category"
    ]
) do conn
    # Register input data
    DuckQuery.register_dataframe(conn, "input_data", data)

    # Calculate aggregations and store in temporary table
    DuckDB.execute(conn, """
        CREATE TEMP TABLE aggregated_data AS
        SELECT
            category,
            COUNT(*) as count,
            MIN(value) as min_value,
            MAX(value) as max_value,
            AVG(value) as avg_value,
            SUM(value) as total_value
        FROM input_data
        GROUP BY category
    """)

end
```

#### Managing Long-Lived Connections

```julia
# Example of using with_connection for multiple operations
# (Requires implementation of with_connection function)

DuckQuery.with_connection("analysis.db", verbose=true) do conn, config
    # First query: create and populate a table
    DuckDB.execute(conn, """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value DOUBLE
        )
    """)

    # Insert data
    for i in 1:10
        DuckDB.execute(conn, """
            INSERT INTO metrics VALUES ($i, 'Metric-$i', $(rand() * 100))
        """)
    end

    # Create a view for filtered data
    DuckDB.execute(conn, """
        CREATE VIEW IF NOT EXISTS high_metrics AS
        SELECT * FROM metrics WHERE value > 50
    """)

    # Query the view
    high_values = DuckQuery.execute_query(conn, "SELECT * FROM high_metrics", config)
    println("High metrics: ", high_values)

    # Create an index for better performance
    DuckDB.execute(conn, "CREATE INDEX IF NOT EXISTS idx_metric_value ON metrics(value)")

    # Run a final query
    result = DuckQuery.execute_query(conn, """
        SELECT
            AVG(value) as avg_value,
            COUNT(*) as total_count,
            COUNT(CASE WHEN value > 50 THEN 1 END) as high_count
        FROM metrics
    """, config)

    println("Summary: ", result)

    # Return the final result
    return result
end
```

### Data Processing Pipelines

#### Using Preprocessors and Postprocessors

```julia
using DuckQuery
using DataFrames
using Dates

# Create a sample DataFrame
df = DataFrame(
    id = 1:10,
    value = rand(10),
    timestamp = now() .- Day.(1:10),
    status = rand(["active", "inactive", "pending"], 10)
)

# Define preprocessing function to modify query
function add_timestamp_filtering(query)
    # Add a timestamp condition if not present
    if !occursin("WHERE", uppercase(query))
        return query * " WHERE timestamp > '$(now() - Day(7))'"
    else
        return query * " AND timestamp > '$(now() - Day(7))'"
    end
end

# Define postprocessing functions
function add_derived_columns(result_df)
    if :value in propertynames(result_df)
        result_df[!, :value_squared] = result_df.value .^ 2
    end
    return result_df
end

function filter_low_values(result_df)
    return filter(row -> row.value > 0.3, result_df)
end

# Use both preprocessors and postprocessors
results = querydf(
    df,
    "SELECT id, value, status FROM df",
    preprocessors = [add_timestamp_filtering],
    postprocessors = [add_derived_columns, filter_low_values],
    verbose = true
)

# Example with complex query transformation
sales_data = DataFrame(
    date = Date(2023,1,1):Day(1):Date(2023,3,31),
    sales = rand(50:500, 90),
    region = rand(["North", "South", "East", "West"], 90)
)

# Preprocessor that adds date filtering based on the current month
function filter_current_month(query)
    current_month = month(today())
    current_year = year(today())
    return replace(
        query,
        "FROM sales_data" =>
        "FROM sales_data WHERE EXTRACT(MONTH FROM date) = $current_month AND EXTRACT(YEAR FROM date) = $current_year"
    )
end

# Postprocessor that adds calculated columns
function add_performance_metrics(df)
    if !isempty(df) && :sales in propertynames(df)
        df[!, :performance] = map(df.sales) do sales
            if sales > 400
                return "Excellent"
            elseif sales > 300
                return "Good"
            elseif sales > 200
                return "Average"
            else
                return "Poor"
            end
        end
    end
    return df
end

# Use the processing pipeline
monthly_analysis = querydf(
    sales_data,
    """
    SELECT
        region,
        AVG(sales) as avg_sales,
        MAX(sales) as max_sales
    FROM sales_data
    GROUP BY region
    ORDER BY avg_sales DESC
    """,
    preprocessors = [filter_current_month],
    postprocessors = [add_performance_metrics]
)
```

### Advanced SQL Features

#### Common Table Expressions (WITH Clauses)

```julia
using DuckQuery
using DataFrames
using Dates

# Create sample data
orders = DataFrame(
    id = 1:100,
    customer_id = rand(1:20, 100),
    order_date = [Date(2023, 1, 1) + Day(rand(0:90)) for _ in 1:100],
    total_amount = rand(50:500, 100)
)

# Use CTEs for complex multi-step analysis
results = querydf(
    Dict("orders" => orders),
    """
    WITH
    -- Calculate monthly sales
    monthly_sales AS (
        SELECT
            EXTRACT(MONTH FROM order_date) AS month,
            EXTRACT(YEAR FROM order_date) AS year,
            SUM(total_amount) AS monthly_revenue,
            COUNT(*) AS order_count
        FROM orders
        GROUP BY month, year
    ),

    -- Calculate customer spending segments
    customer_segments AS (
        SELECT
            customer_id,
            SUM(total_amount) AS total_spent,
            CASE
                WHEN SUM(total_amount) > 2000 THEN 'High'
                WHEN SUM(total_amount) > 1000 THEN 'Medium'
                ELSE 'Low'
            END AS segment
        FROM orders
        GROUP BY customer_id
    )

    -- Main query using both CTEs
    SELECT
        cs.segment,
        COUNT(DISTINCT cs.customer_id) AS customer_count,
        SUM(ms.monthly_revenue) AS total_revenue,
        AVG(ms.monthly_revenue) AS avg_monthly_revenue
    FROM customer_segments cs
    JOIN orders o ON cs.customer_id = o.customer_id
    JOIN monthly_sales ms ON
        EXTRACT(MONTH FROM o.order_date) = ms.month AND
        EXTRACT(YEAR FROM o.order_date) = ms.year
    GROUP BY cs.segment
    ORDER BY total_revenue DESC
    """
)
```

#### Window Functions and Advanced Analytics

```julia
using DuckQuery
using DataFrames
using Dates

# Create sample data
stock_prices = DataFrame(
    date = Date(2023,1,1):Day(1):Date(2023,3,31),
    symbol = repeat(["AAPL", "MSFT", "GOOGL", "AMZN"], inner=23),
    price = [100 + 5*sin(i) + rand(-3:0.1:3) for i in 1:90]
)

# Use window functions for time series analysis
analysis = querydf(
    stock_prices,
    """
    SELECT
        symbol,
        date,
        price,
        -- Calculate moving averages
        AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7day,

        -- Calculate price changes
        price - LAG(price, 1) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS daily_change,

        -- Calculate percent changes
        (price - LAG(price, 1) OVER (
            PARTITION BY symbol
            ORDER BY date
        )) / LAG(price, 1) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) * 100 AS daily_percent_change,

        -- Calculate ranks
        RANK() OVER (
            PARTITION BY symbol
            ORDER BY price DESC
        ) AS price_rank,

        -- Calculate percentiles
        NTILE(4) OVER (
            PARTITION BY symbol
            ORDER BY price
        ) AS price_quartile
    FROM df
    ORDER BY symbol, date
    """
)
```

### Error Handling

#### Using Different Error Strategies

```julia
using DuckQuery
using DataFrames

# Create a sample DataFrame with potential issues
df = DataFrame(
    id = 1:5,
    value = [10.5, missing, 20.3, 15.7, missing],
    name = ["Item A", "Item B", "Item C", "Item D", "Item E"]
)

# Default error handling (throw exception)
try
    result = querydf(
        df,
        "SELECT id, value / 0 AS impossible_value FROM df"
    )
catch e
    println("Error caught: ", e)
end

# Return empty DataFrame on error
empty_result = querydf(
    df,
    "SELECT id, SQRT(value) FROM df WHERE value < 0",
    on_error = :return_empty
)
println("Result when error occurs with on_error=:return_empty:")
println(empty_result)

# Log error and return empty DataFrame
log_result = querydf(
    df,
    "SELECT * FROM nonexistent_table",
    on_error = :log,
    verbose = true
)

# Combine with preprocessors for safer queries
function make_query_safe(query)
    # Add handling for missing values
    return replace(
        query,
        "SELECT" => "SELECT COALESCE(value, 0) AS value_safe,"
    )
end

safe_result = querydf(
    df,
    "SELECT id, value FROM df",
    preprocessors = [make_query_safe],
    on_error = :log
)
```

### Performance Optimization

#### Configuring DuckDB for Performance

```julia
using DuckQuery
using DataFrames
using BenchmarkTools

# Generate a larger dataset for testing
large_df = DataFrame(
    id = 1:100_000,
    value_a = rand(100_000),
    value_b = rand(100_000),
    category = rand(["A", "B", "C", "D", "E"], 100_000)
)

# Basic query without optimization
@time basic_result = querydf(
    large_df,
    """
    SELECT
        category,
        COUNT(*) as count,
        AVG(value_a) as avg_a,
        AVG(value_b) as avg_b
    FROM df
    GROUP BY category
    """
)

# Same query with performance configuration
@time optimized_result = querydf(
    large_df,
    """
    SELECT
        category,
        COUNT(*) as count,
        AVG(value_a) as avg_a,
        AVG(value_b) as avg_b
    FROM df
    GROUP BY category
    """,
    init_config = Dict{Symbol, Any}(
        :memory_limit => "4GB",
        :threads => 8
    ),
    profile = true
)

# Using profiling to optimize queries
complex_query_result = querydf(
    Dict("data" => large_df),
    """
    SELECT
        category,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY value_a) AS median_a,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY value_b) AS median_b,
        CORR(value_a, value_b) AS correlation
    FROM data
    GROUP BY category
    """,
    profile = true
)
```

## API Reference

### Main Functions

#### querydf

```julia
# Database file with a single query
querydf(dbfile::String, query::String; kwargs...)::DataFrame

# Database file with multiple queries
querydf(dbfile::String, queries::Vector{String}; kwargs...)::DataFrame

# DataFrame as source
querydf(df::DataFrame, query::String; kwargs...)::DataFrame

# Multiple DataFrames as sources
querydf(dfs::Dict{String, DataFrame}, query::String; kwargs...)::DataFrame

# Mixed sources (database files and DataFrames)
querydf(sources::Dict{String, <:Any}, query::String; kwargs...)::DataFrame

# Do-block variants for connection control
querydf(f::Function, dbfile::String, query::String; kwargs...)::DataFrame
querydf(f::Function, dbfile::String, queries::Vector{String}; kwargs...)::DataFrame
```

### Common Keyword Arguments

- `init_queries::Union{String, Vector{String}}`: Initialization queries to run before the main query
- `init_config::Dict{Symbol, Any}`: Configuration settings for DuckDB
- `verbose::Bool`: Enable verbose logging
- `profile::Bool`: Enable query profiling
- `preprocessors::Vector{<:Function}`: Functions to transform the query before execution
- `postprocessors::Vector{<:Function}`: Functions to transform the result DataFrame
- `on_error::Symbol`: Error handling strategy (`:throw`, `:return_empty`, `:log`)

### Configuration Options

The `init_config` dictionary can include:

- `:memory_limit`: Maximum memory usage (e.g., "4GB")
- `:threads`: Number of threads to use
- `:extensions`: Extensions to load (e.g., ["parquet", "json", "httpfs"])

## Contributing

Contributions to DuckQuery.jl are welcome! Please feel free to:

1. Open issues for bugs or feature requests
2. Submit pull requests with improvements
3. Add more examples or improve documentation

## License

DuckQuery.jl is available under the MIT License. See the LICENSE file for more details.
