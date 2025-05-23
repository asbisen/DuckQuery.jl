module DuckQuery

export querydf

using DataFrames
using DuckDB
using Dates
using Printf

include("types.jl")
include("source/source_manager.jl")
include("utils.jl")
include("core.jl")

end # module DuckQuery
