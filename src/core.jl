# Core functionality implementing the querydf methods

"""
    querydf(sources::Dict{String, <:Any}, query::String; kwargs...)::DataFrame
    querydf(dbfile::String, query::String; kwargs...)::DataFrame
    querydf(dbfile::String, queries::Vector{String}; kwargs...)::DataFrame
    querydf(df::DataFrame, query::String; kwargs...)::DataFrame
    querydf(dfs::Dict{String, DataFrame}, query::String; kwargs...)::DataFrame
    querydf(f::Function, dbfile::String, query::String; kwargs...)::DataFrame
    querydf(f::Function, dbfile::String, queries::Vector{String}; kwargs...)::DataFrame

Execute SQL queries against various data sources (DataFrames, database files, or a mix of both) using DuckDB.

# Arguments
- `sources::Dict{String, <:Any}`: Named sources (DataFrames or database file paths)
- `dbfile::String`: Path to the database file or ":memory:" for in-memory database
- `queries::Vector{String}`: List of SQL queries to execute in sequence
- `query::String`: SQL query to execute
- `df::DataFrame`: DataFrame to query
- `dfs::Dict{String, DataFrame}`: Named DataFrames to query
- `f::Function`: Function that takes a DuckDB connection and performs additional operations

# Keyword Arguments
- `init_queries::Union{String,Vector{String}}=String[]`: Initial SQL queries to execute before the main query
- `init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}()`: Configuration options for DuckDB, such as:
  - `:threads => n`: Number of threads to use
  - `:memory_limit => "4GB"`: Memory limit for DuckDB
  - Other DuckDB configuration options as key-value pairs
- `verbose::Bool=false`: Whether to print verbose output
- `profile::Bool=false`: Whether to measure and display execution time
- `preprocessors::Vector{<:Function}=Function[]`: Functions that transform the query before execution
- `postprocessors::Vector{<:Function}=Function[]`: Functions that transform the result DataFrame
- `on_error::Symbol=:throw`: Error handling behavior. Options:
  - `:throw`: Throw an exception on error (default)
  - `:return_empty`: Return an empty DataFrame on error
  - `:log`: Log the error and return an empty DataFrame
- `readonly::Bool=false`: Whether to open database files in read-only mode

# Returns
- `DataFrame`: Result of the query (or the last query in a sequence)

# Examples

## Query an in-memory database
```julia
# Basic query on an in-memory database
result = querydf(":memory:", "SELECT 1 AS one, 'test' AS text")

# Multiple queries in sequence
result = querydf(
    ":memory:",
    [
        "CREATE TABLE test (id INTEGER, name VARCHAR)",
        "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')",
        "SELECT * FROM test WHERE id = 2"
    ]
)
```

## Query a DataFrame
```julia
# Query a single DataFrame
df = DataFrame(id=[1, 2, 3], name=["Alice", "Bob", "Charlie"])
result = querydf(df, "SELECT * FROM df WHERE id > 1")

# Query multiple DataFrames with joins
customers = DataFrame(id=[1, 2, 3], name=["Alice", "Bob", "Charlie"])
orders = DataFrame(id=[101, 102], customer_id=[1, 3], amount=[100, 200])

result = querydf(
    Dict("customers" => customers, "orders" => orders),
    \"\"\"
    SELECT c.name, o.amount
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    ORDER BY o.amount DESC
    \"\"\"
)
```

## Query external data sources
```julia
# Query a Parquet file from a URL
datauri = "https://example.com/data.parquet"
df = querydf(":memory:", "SELECT * FROM '\$datauri' USING SAMPLE 10000")

# Query a CSV file
result = querydf(":memory:", "SELECT * FROM 'data.csv'")
```

## Persist data to a database file
```julia
# Save query results to a database file
dbfile = "local.db"
df = DataFrame(id=[1, 2, 3], value=[10.5, 20.5, 30.5])

# Create a dictionary with DataFrame and database file
datamap = Dict(
    "df" => df,
    "db" => dbfile
)

# Create a table in the database from the DataFrame
querydf(datamap, "CREATE TABLE db.tbl AS SELECT * FROM df")

# Read from the created table
result = querydf(dbfile, "SELECT * FROM tbl")
```

## Using preprocessing and postprocessing
```julia
df = DataFrame(id=[1, 2, 3], age=[25, 17, 30])

# Preprocess query to replace table name
# Postprocess result to filter adults only
result = querydf(
    df,
    "SELECT * FROM table_name",
    preprocessors=[query -> replace(query, "table_name" => "df")],
    postprocessors=[df -> filter(:age => >(18), df)]
)
```

## Using do-block syntax for connection management
```julia
# Create table, insert data, and query in a single operation
result = querydf("database.db", "SELECT * FROM test_table") do conn
    DuckDB.execute(conn, "CREATE TABLE test_table (id INTEGER, name TEXT)")
    DuckDB.execute(conn, "INSERT INTO test_table VALUES (1, 'Test')")
end
```

## Error handling
```julia
# Default behavior - throws exception
df = DataFrame(id=[1, 2, 3])
# Will throw an exception
# querydf(df, "SELECT * FROM nonexistent_table")

# Return empty DataFrame on error
empty_result = querydf(
    df,
    "SELECT * FROM nonexistent_table",
    on_error=:return_empty
)

# Log error and return empty DataFrame
log_result = querydf(
    df,
    "SELECT * FROM nonexistent_table",
    on_error=:log
)
```

## Configuration options
```julia
# Set number of threads and memory limit
result = querydf(
    ":memory:",
    "SELECT 1 AS one",
    init_config=Dict{Symbol,Any}(
        :threads => 4,
        :memory_limit => "2GB"
    )
)

# Use read-only mode for database files
result = querydf(
    "database.db",
    "SELECT * FROM table",
    readonly=true
)
```
"""
function querydf(
    dbfile::String,
    query::String;
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize connection using the SourceManager
        conn = SourceManager.initialize_connection(dbfile, config)

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# Do-block variant for single query on database file
function querydf(
    f::Function,
    dbfile::String,
    query::String;
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize connection using the SourceManager
        conn = SourceManager.initialize_connection(dbfile, config)

        # Execute the provided function with the connection
        f(conn)

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# This docstring is already covered by the comprehensive docstring above
"""
    querydf(dbfile::String, queries::Vector{String}; kwargs...)::DataFrame

Execute multiple SQL queries in sequence against a DuckDB database file.

# Arguments
- `dbfile::String`: Path to the database file or ":memory:" for in-memory database
- `queries::Vector{String}`: List of SQL queries to execute in sequence
- `kwargs`: See the single query variant for available keyword arguments

# Returns
- `DataFrame`: Result of the last query in the sequence
"""
function querydf(
    dbfile::String,
    queries::Vector{String};
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize connection using the SourceManager
        conn = SourceManager.initialize_connection(dbfile, config)

        # Execute queries
        result = execute_queries(conn, queries, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# Do-block variant for multiple queries on database file
function querydf(
    f::Function,
    dbfile::String,
    queries::Vector{String};
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize connection using the SourceManager
        conn = SourceManager.initialize_connection(dbfile, config)

        # Execute the provided function with the connection
        f(conn)

        # Execute queries
        result = execute_queries(conn, queries, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# This docstring is already covered by the comprehensive docstring above
"""
    querydf(df::DataFrame, query::String; kwargs...)::DataFrame

Execute a SQL query against a DataFrame.

# Arguments
- `df::DataFrame`: DataFrame to query
- `query::String`: SQL query to execute
- `kwargs`: See other variants for available keyword arguments

# Returns
- `DataFrame`: Result of the query
"""
function querydf(
    df::DataFrame,
    query::String;
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize in-memory connection using the SourceManager
        conn = SourceManager.initialize_connection(":memory:", config)

        # Register DataFrame using the SourceManager
        SourceManager.register_source(conn, "df", df, config)

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# This docstring is already covered by the comprehensive docstring above
"""
    querydf(dfs::Dict{String, DataFrame}, query::String; kwargs...)::DataFrame

Execute a SQL query against multiple DataFrames.

# Arguments
- `dfs::Dict{String, DataFrame}`: Named DataFrames to query
- `query::String`: SQL query to execute
- `kwargs`: See other variants for available keyword arguments

# Returns
- `DataFrame`: Result of the query
"""
function querydf(
    dfs::Dict{String,DataFrame},
    query::String;
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize in-memory connection using the SourceManager
        conn = SourceManager.initialize_connection(":memory:", config)

        # Register all DataFrames using the SourceManager
        for (name, df) in dfs
            SourceManager.register_source(conn, name, df, config)
        end

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end


# This docstring is already covered by the comprehensive docstring above
"""
    querydf(sources::Dict{String, <:Any}, query::String; kwargs...)::DataFrame

Execute a SQL query against a mix of DataFrames and database files.

# Arguments
- `sources::Dict{String, <:Any}`: Named sources (DataFrames or database file paths)
- `query::String`: SQL query to execute
- `kwargs`: See other variants for available keyword arguments

# Returns
- `DataFrame`: Result of the query
"""
function querydf(
    sources::Dict{String,<:Any},
    query::String;
    init_queries::Union{String,Vector{String}}=String[],
    init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
    verbose::Bool=false,
    profile::Bool=false,
    preprocessors::Vector{<:Function}=Function[],
    postprocessors::Vector{<:Function}=Function[],
    on_error::Symbol=:throw,
    readonly::Bool=false
)::DataFrame
    config = DuckQueryConfig(
        init_queries, init_config, verbose, profile,
        preprocessors, postprocessors, on_error, readonly
    )

    if profile
        total_start_time = Dates.now()
    end

    result = nothing
    conn = nothing

    try
        # Initialize in-memory connection using the SourceManager
        conn = SourceManager.initialize_connection(":memory:", config)

        # Register all sources using the SourceManager
        for (name, source) in sources
            SourceManager.register_source(conn, name, source, config)
        end

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection using the SourceManager
        if conn !== nothing
            SourceManager.close_connection(conn, config.verbose)
        end
    end
end
