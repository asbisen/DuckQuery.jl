# Core functionality implementing the querydf methods

"""
    querydf(sources::Dict{String, <:Any}, query::String; kwargs...)::DataFrame

Execute a SQL query against a mix of DataFrames and database files.

# Arguments
- `sources::Dict{String, <:Any}`: Named sources (DataFrames or database file paths)
- `query::String`: SQL query to execute
- `kwargs`: See other variants for available keyword arguments, including `readonly` which, when true,
  will open all database files specified in `sources` in read-only mode

# Returns
- `DataFrame`: Result of the query
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
        # Initialize connection
        conn = initialize_connection(dbfile, config)

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
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
        # Initialize connection
        conn = initialize_connection(dbfile, config)

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
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
        end
    end
end


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
        # Initialize connection
        conn = initialize_connection(dbfile, config)

        # Execute queries
        result = execute_queries(conn, queries, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
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
        # Initialize connection
        conn = initialize_connection(dbfile, config)

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
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
        end
    end
end


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
        # Initialize in-memory connection
        conn = initialize_connection(":memory:", config)

        # Register DataFrame
        register_dataframe(conn, "df", df)

        # Execute query
        result = execute_query(conn, query, config)

        if profile
            total_end_time = Dates.now()
            total_duration_ms = Dates.value(total_end_time - total_start_time)
            @printf "[Total Time] %.2f ms\n" total_duration_ms
        end

        return result
    finally
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
        end
    end
end


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
        # Initialize in-memory connection
        conn = initialize_connection(":memory:", config)

        # Register all DataFrames
        for (name, df) in dfs
            register_dataframe(conn, name, df)
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
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
        end
    end
end


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
        # Initialize in-memory connection
        conn = initialize_connection(":memory:", config)

        # Register all sources
        for (name, source) in sources
            if isa(source, DataFrame)
                # Register DataFrame directly
                register_dataframe(conn, name, source)
            elseif isa(source, String)
                # Attach database file with read-only flag if specified
                if config.readonly
                    attach_query = "ATTACH '$(source)' AS $(name) (READ_ONLY)"
                else
                    attach_query = "ATTACH '$(source)' AS $(name)"
                end
                log_message("Attaching database: $attach_query", config.verbose)
                DuckDB.execute(conn, attach_query)
            else
                error("Unsupported source type: $(typeof(source))")
            end
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
        # Always close connection
        if conn !== nothing
            log_message("Closing connection", config.verbose)
            close(conn)
        end
    end
end
