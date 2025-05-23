# Utility functions for DuckQuery

"""
    log_message(message::String, verbose::Bool)

Log a message if verbose mode is enabled.
"""
function log_message(message::String, verbose::Bool)
    if verbose
        @printf "[DuckQuery] %s\n" message
    end
end

"""
    initialize_connection(source::String, config::DuckQueryConfig)

Initialize a DuckDB connection to the given source with the specified configuration.

This is a compatibility wrapper around SourceManager.initialize_connection.
"""
function initialize_connection(source::String, config::DuckQueryConfig)
    log_message("Initializing connection to $source", config.verbose)
    return SourceManager.initialize_connection(source, config)
end



"""
    register_dataframe(conn::DuckDB.DB, name::String, df::DataFrame)

Register a DataFrame in the DuckDB connection with a given name.

This is a compatibility wrapper around SourceManager.register_dataframe.
"""
function register_dataframe(conn::DuckDB.DB, name::String, df::DataFrame)
    # Create a basic config to pass to SourceManager
    config = DuckQueryConfig()
    return SourceManager.register_dataframe(conn, name, df, config)
end

# Helper function to convert Julia types to DuckDB types
# This is a compatibility wrapper around SourceManager.get_duckdb_type
function get_duckdb_type(type::Type)
    return SourceManager.get_duckdb_type(type)
end

# Helper function to format values for SQL
# Kept for backward compatibility
function format_value_for_sql(value)
    if value === missing
        return "NULL"
    elseif value isa AbstractString
        # Escape single quotes and wrap in single quotes
        return "'$(replace(string(value), "'" => "''"))'"
    elseif value isa Bool
        return value ? "TRUE" : "FALSE"
    elseif value isa Dates.Date
        return "'$(value)'"
    elseif value isa Dates.DateTime
        return "'$(value)'"
    else
        return string(value)
    end
end

"""
    execute_query(conn::DuckDB.DB, query::String, config::DuckQueryConfig)

Execute a single query with timing information if profiling is enabled.
"""
function execute_query(conn::DuckDB.DB, query::String, config::DuckQueryConfig)
    processed_query = query

    # Apply preprocessors
    for preprocessor in config.preprocessors
        processed_query = preprocessor(processed_query)
    end

    log_message("Executing query: $(processed_query[1:min(30, length(processed_query))])...", config.verbose)

    start_time = nothing
    result_df = nothing

    try
        if config.profile
            start_time = Dates.now()
            result_df = DuckDB.execute(conn, processed_query) |> DataFrame
            end_time = Dates.now()
            duration_ms = Dates.value(end_time - start_time)
            @printf "[Query Time] %.2f ms\n" duration_ms
        else
            result_df = DuckDB.execute(conn, processed_query) |> DataFrame
        end

        # Apply postprocessors
        for postprocessor in config.postprocessors
            result_df = postprocessor(result_df)
        end

        return result_df
    catch e
        if config.on_error == :throw
            # If it's already a DuckDB.QueryException, rethrow it directly
            if isa(e, DuckDB.QueryException)
                throw(e)
            else
                throw(DuckQueryError("Failed to execute query", query, e))
            end
        elseif config.on_error == :return_empty
            log_message("Error executing query, returning empty DataFrame: $(e)", true)
            return DataFrame()
        elseif config.on_error == :log
            log_message("Error executing query: $(e)", true)
            return DataFrame()
        end
    end
end

"""
    execute_queries(conn::DuckDB.DB, queries::Vector{String}, config::DuckQueryConfig)

Execute multiple queries in sequence, returning the result of the last query.
"""
function execute_queries(conn::DuckDB.DB, queries::Vector{String}, config::DuckQueryConfig)
    results = nothing

    for (i, query) in enumerate(queries)
        log_message("Executing query $i of $(length(queries))", config.verbose)
        results = execute_query(conn, query, config)
    end

    return results
end
