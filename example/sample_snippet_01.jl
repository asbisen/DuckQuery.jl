# ---
# jupyter:
#   jupytext:
#     formats: jl:percent
#     text_representation:
#       extension: .jl
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Julia 1.11.5
#     language: julia
#     name: julia-1.11
# ---

# %%
using DuckQuery
using DataFrames

# local database file to use to persist data
dbfile = "local.db"

# %%
# query parquet file consuming from an https location
datauri = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
df = querydf(dbfile, "SELECT * FROM '$datauri' USING SAMPLE 10000")
first(df, 4) # show top 4 rows

# %%
# query a dataframe and persist the data to a local file
# create a 'datamap' dictionary with name of a dataframe
# and a local file
datamap = Dict(
    "df" => df,
    "db" => dbfile
)

qry = """
    CREATE TABLE db.tbl AS
        SELECT tpep_pickup_datetime, passenger_count, trip_distance 
        FROM df
"""
querydf(datamap, qry)

# %%
