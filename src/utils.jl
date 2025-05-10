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
"""
function initialize_connection(source::String, config::DuckQueryConfig)
    log_message("Initializing connection to $source", config.verbose)
    
    # Create connection
    conn = DuckDB.DB(source)
    
    # Apply configuration
    for (key, value) in config.init_config
        key_str = String(key)
        value_str = string(value)
        log_message("Setting config: $key_str = $value_str", config.verbose)
        
        # For memory limit and similar settings
        if key == :memory_limit
            DuckDB.execute(conn, "SET memory_limit='$(value)'")  
        elseif key == :threads
            DuckDB.execute(conn, "SET threads=$(value)")
        # Handle other configuration settings
        elseif key == :extensions
            if isa(value, Vector)
                for extension in value
                    DuckDB.execute(conn, "INSTALL $(extension); LOAD $(extension)")
                end
            else
                DuckDB.execute(conn, "INSTALL $(value); LOAD $(value)")
            end
        end
    end
    
    # Execute initialization queries
    for query in config.init_queries
        log_message("Executing init query: $(query[1:min(30, length(query))])...", config.verbose)
        DuckDB.execute(conn, query)
    end
    
    return conn
end

"""
    register_dataframe(conn::DuckDB.DB, name::String, df::DataFrame)

Register a DataFrame in the DuckDB connection with a given name.
"""
function register_dataframe(conn::DuckDB.DB, name::String, df::DataFrame)
    # Simplified approach: Create a table from the DataFrame manually
    # First, create a temporary table with the appropriate columns
    if isempty(df)
        # Handle empty dataframe case
        columns = names(df)
        types = [eltype(df[!, col]) for col in columns]
        schema = join(["\"$(columns[i])\" $(get_duckdb_type(types[i]))" for i in 1:length(columns)], ", ")
        DuckDB.execute(conn, "CREATE TEMPORARY TABLE $(name) ($schema)")
        return
    end
    
    # For non-empty dataframes, create the table with data
    columns = names(df)
    types = [eltype(df[!, col]) for col in columns]
    
    # Create a table from the DataFrame
    schema = join(["\"$(columns[i])\" $(get_duckdb_type(types[i]))" for i in 1:length(columns)], ", ")
    DuckDB.execute(conn, "CREATE TEMPORARY TABLE $(name) ($schema)")
    
    # Insert data into the table
    for row in eachrow(df)
        values = [format_value_for_sql(row[col]) for col in columns]
        DuckDB.execute(conn, "INSERT INTO $(name) VALUES ($(join(values, ", ")))")
    end
end

# Helper function to convert Julia types to DuckDB types
function get_duckdb_type(type::Type)
    if type <: Integer
        return "INTEGER"
    elseif type <: AbstractFloat
        return "DOUBLE"
    elseif type <: AbstractString
        return "VARCHAR"
    elseif type <: Bool
        return "BOOLEAN"
    elseif type <: Dates.Date
        return "DATE"
    elseif type <: Dates.DateTime
        return "TIMESTAMP"
    else
        return "VARCHAR"
    end
end

# Helper function to format values for SQL
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
            throw(DuckQueryError("Failed to execute query", query, e))
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