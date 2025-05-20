# Custom error types for DuckQuery
struct DuckQueryError <: Exception
    message::String
    query::Union{String,Vector{String}}
    cause::Exception
end

Base.showerror(io::IO, e::DuckQueryError) = print(io, "DuckQueryError: ", e.message, "\nQuery: ", e.query, "\nCause: ", e.cause)

# Configuration type
struct DuckQueryConfig
    init_queries::Vector{String}
    init_config::AbstractDict{Symbol,<:Any}
    verbose::Bool
    profile::Bool
    preprocessors::Vector{<:Function}
    postprocessors::Vector{<:Function}
    on_error::Symbol
    readonly::Bool

    # Constructor with defaults
    function DuckQueryConfig(
        init_queries::Union{String,Vector{String}}=String[],
        init_config::AbstractDict{Symbol,<:Any}=Dict{Symbol,Any}(),
        verbose::Bool=false,
        profile::Bool=false,
        preprocessors::Vector{<:Function}=Function[],
        postprocessors::Vector{<:Function}=Function[],
        on_error::Symbol=:throw,
        readonly::Bool=false
    )
        # Convert single string to vector if needed
        init_queries_vec = isa(init_queries, String) ? [init_queries] : init_queries

        # Validate on_error value
        if !(on_error in [:throw, :return_empty, :log])
            throw(ArgumentError("on_error must be one of :throw, :return_empty, or :log"))
        end

        new(init_queries_vec, init_config, verbose, profile, preprocessors, postprocessors, on_error, readonly)
    end
end
