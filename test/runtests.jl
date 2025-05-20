using DuckQuery
using DataFrames
using Test
using Dates
using Random
using DuckDB

@testset "DuckQuery.jl" begin
    # Test querydf with in-memory database
    @testset "In-memory database" begin
        result = querydf(
            ":memory:",
            "SELECT 1 AS one, 'test' AS text"
        )
        @test size(result) == (1, 2)
        @test result[1, :one] == 1
        @test result[1, :text] == "test"
    end

    # Test querydf with DataFrame
    @testset "DataFrame query" begin
        df = DataFrame(id=[1, 2, 3], name=["Alice", "Bob", "Charlie"])
        result = querydf(
            df,
            "SELECT * FROM df WHERE id > 1"
        )
        @test size(result) == (2, 2)
        @test result[1, :id] == 2
        @test result[2, :name] == "Charlie"
    end

    # Test querydf with multiple queries
    @testset "Multiple queries" begin
        result = querydf(
            ":memory:",
            [
                "CREATE TABLE test (id INTEGER, name VARCHAR)",
                "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')",
                "SELECT * FROM test WHERE id = 2"
            ]
        )
        @test size(result) == (1, 2)
        @test result[1, :id] == 2
        @test result[1, :name] == "Bob"
    end

    # Test querydf with multiple DataFrames
    @testset "Multiple DataFrames" begin
        customers = DataFrame(id=[1, 2, 3], name=["Alice", "Bob", "Charlie"])
        orders = DataFrame(id=[101, 102], customer_id=[1, 3], amount=[100, 200])

        result = querydf(
            Dict("customers" => customers, "orders" => orders),
            """
            SELECT c.name, o.amount
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            ORDER BY o.amount DESC
            """
        )

        @test size(result) == (2, 2)
        @test result[1, :name] == "Charlie"
        @test result[1, :amount] == 200
    end

    # Test preprocessors and postprocessors
    @testset "Preprocessors and postprocessors" begin
        df = DataFrame(id=[1, 2, 3], age=[25, 17, 30])

        result = querydf(
            df,
            "SELECT * FROM table_name",
            preprocessors=[query -> replace(query, "table_name" => "df")],
            postprocessors=[df -> filter(:age => >(18), df)]
        )

        @test size(result) == (2, 2)
        @test result[1, :age] == 25
        @test result[2, :age] == 30
    end

    # Test database persistence and reading from file
    @testset "Database file persistence" begin
        # Create a temporary database file
        db_file = tempname() * ".duckdb"

        try
            # Create a test DataFrame
            original_df = DataFrame(
                id=[1, 2, 3, 4, 5],
                name=["Alice", "Bob", "Charlie", "David", "Eve"],
                score=[95.5, 87.2, 92.0, 78.5, 88.9],
                active=[true, false, true, true, false]
            )

            # Save the DataFrame to the database file
            querydf(
                db_file,
                [
                    "CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE, active BOOLEAN)",
                    """INSERT INTO users
                       SELECT * FROM df
                    """
                ],
                init_config=Dict{Symbol,Any}(:memory_limit => "100MB"),
                verbose=true
            ) do conn
                # Register the DataFrame directly in the passed connection
                DuckQuery.register_dataframe(conn, "df", original_df)
            end

            # Read the data back from the database file
            result = querydf(
                db_file,
                "SELECT * FROM users ORDER BY id"
            )

            # Verify the data matches the original DataFrame
            @test size(result) == size(original_df)
            @test result[!, :id] == original_df[!, :id]
            @test result[!, :name] == original_df[!, :name]
            @test result[!, :score] ≈ original_df[!, :score] atol = 1e-5
            @test result[!, :active] == original_df[!, :active]

            # Test a more complex query on the database file
            filtered_result = querydf(
                db_file,
                "SELECT name, score FROM users WHERE active = true ORDER BY score DESC"
            )

            @test size(filtered_result) == (3, 2)
            # Sort by score in descending order, so Alice (95.5) is first, then Charlie (92.0), then David (78.5)
            @test filtered_result[1, :name] == "Alice"
            @test filtered_result[1, :score] ≈ 95.5 atol = 1e-5
            @test filtered_result[2, :name] == "Charlie"
            @test filtered_result[3, :name] == "David"
        finally
            # Clean up the temporary database file
            if isfile(db_file)
                rm(db_file, force=true)
            end
        end
    end

    # 1. Error Handling
    @testset "Error handling" begin
        # Test :throw behavior (default)
        df = DataFrame(id=[1, 2, 3])
        @test_throws Exception querydf(df, "SELECT * FROM nonexistent_table")

        # Test :return_empty behavior
        empty_result = querydf(df, "SELECT * FROM nonexistent_table", on_error=:return_empty)
        @test isempty(empty_result)

        # Test :log behavior
        log_result = querydf(df, "SELECT * FROM nonexistent_table", on_error=:log)
        @test isempty(log_result)
    end

    # 2. Mixed Data Sources
    @testset "Mixed sources" begin
        # Create a temporary database file
        db_file = tempname() * ".duckdb"

        try
            # Create a table in the database
            querydf(db_file, "CREATE TABLE db_table (id INTEGER, value DOUBLE)")
            querydf(db_file, "INSERT INTO db_table VALUES (1, 10.5), (2, 20.5)")

            # Create a DataFrame
            df = DataFrame(id=[2, 3], name=["Bob", "Charlie"])

            # Query across both sources
            result = querydf(
                Dict("db_source" => db_file, "df_source" => df),
                """
                SELECT d.id, d.name, db.value
                FROM df_source d
                LEFT JOIN db_source.db_table db ON d.id = db.id
                ORDER BY d.id
                """
            )

            @test size(result) == (2, 3)
            @test result[1, :id] == 2
            @test result[1, :value] == 20.5
            @test ismissing(result[2, :value]) || result[2, :value] === nothing
        finally
            # Clean up
            isfile(db_file) && rm(db_file, force=true)
        end
    end

    # 3. Profiling Functionality
    @testset "Profiling" begin
        df = DataFrame(id=1:100, value=rand(100))

        # Test that profiling doesn't affect results
        result_without_profile = querydf(df, "SELECT AVG(value) as avg FROM df")
        result_with_profile = querydf(df, "SELECT AVG(value) as avg FROM df", profile=true)

        @test result_without_profile[1, :avg] ≈ result_with_profile[1, :avg] atol = 1e-6
    end

    # 4. Configuration Options
    @testset "Configuration options" begin

        num_threads = Threads.nthreads()
        # Test threads configuration
        result = querydf(
            ":memory:",
            "SELECT 1 AS one",
            init_config=Dict{Symbol,Any}(:threads => num_threads)
        )
        @test result[1, :one] == 1

        # Test memory limit configuration
        result = querydf(
            ":memory:",
            "SELECT 1 AS one",
            init_config=Dict{Symbol,Any}(:memory_limit => "100MB")
        )
        @test result[1, :one] == 1
    end

    # 5. Edge Cases
    @testset "Edge cases" begin
        # Empty DataFrame
        empty_df = DataFrame(a=Int[], b=String[])
        empty_result = querydf(empty_df, "SELECT * FROM df")
        @test isempty(empty_result)

        # DataFrame with missing values
        df_with_missing = DataFrame(id=[1, 2, 3], value=[10, missing, 30])
        missing_result = querydf(df_with_missing, "SELECT * FROM df WHERE value IS NOT NULL")
        @test size(missing_result) == (2, 2)
    end

    # 6. Do-Block Variants
    @testset "Do-block variants" begin
        # Test do-block with database file
        db_file = tempname() * ".duckdb"

        try
            # Using do-block to control connection
            result = querydf(db_file, "SELECT * FROM test_table") do conn
                DuckDB.execute(conn, "CREATE TABLE test_table (id INTEGER, name TEXT)")
                DuckDB.execute(conn, "INSERT INTO test_table VALUES (1, 'Test')")
            end

            @test size(result) == (1, 2)
            @test result[1, :id] == 1
            @test result[1, :name] == "Test"

            # Using do-block with multiple queries
            result = querydf(db_file, ["SELECT COUNT(*) as count FROM test_table", "SELECT * FROM test_table"]) do conn
                DuckDB.execute(conn, "INSERT INTO test_table VALUES (2, 'Second')")
            end

            @test size(result) == (2, 2)
        finally
            isfile(db_file) && rm(db_file, force=true)
        end
    end

    # 7. Large DataFrames and Performance
    @testset "Large DataFrames" begin
        # Create a larger DataFrame - smaller than suggested to avoid slowing down tests too much
        Random.seed!(42) # For reproducibility
        large_df = DataFrame(
            id=1:1000,
            value=rand(1000),
            category=rand(["A", "B", "C", "D"], 1000)
        )

        # Test it works with larger data
        result = querydf(
            large_df,
            """
            SELECT
                category,
                COUNT(*) as count,
                AVG(value) as avg
            FROM df
            GROUP BY category
            """
        )

        @test size(result) == (4, 3)
        @test sum(result[!, :count]) == 1000
    end

    # 8. Initialization Queries
    @testset "Initialization queries" begin
        # Test single init query
        result = querydf(
            ":memory:",
            "SELECT * FROM test_table",
            init_queries="CREATE TABLE test_table AS SELECT 1 AS id, 'Test' AS name"
        )
        @test size(result) == (1, 2)

        # Test multiple init queries
        result = querydf(
            ":memory:",
            "SELECT * FROM test_table WHERE id > 1",
            init_queries=[
                "CREATE TABLE test_table (id INTEGER, name TEXT)",
                "INSERT INTO test_table VALUES (1, 'One'), (2, 'Two'), (3, 'Three')"
            ]
        )
        @test size(result) == (2, 2)
    end

    # Test read-only mode functionality
    @testset "Read-only mode" begin
        # Create a temporary database file
        db_file = tempname() * ".duckdb"

        try
            # Create and populate a test database
            querydf(
                db_file,
                [
                    "CREATE TABLE test_ro (id INTEGER, name VARCHAR)",
                    "INSERT INTO test_ro VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
                ]
            )

            # Test read-only query works
            result = querydf(
                db_file,
                "SELECT * FROM test_ro ORDER BY id",
                readonly=true
            )

            @test size(result) == (3, 2)
            @test result[1, :name] == "Alice"
            @test result[3, :name] == "Charlie"

            # Test write operation fails in read-only mode
            @test_throws Exception querydf(
                db_file,
                "INSERT INTO test_ro VALUES (4, 'Dave')",
                readonly=true
            )

            # Test write operation succeeds in normal mode
            querydf(
                db_file,
                "INSERT INTO test_ro VALUES (4, 'Dave')"
            )

            result = querydf(
                db_file,
                "SELECT * FROM test_ro WHERE id = 4",
                readonly=true
            )

            @test size(result) == (1, 2)
            @test result[1, :name] == "Dave"

            # Test readonly with do-block
            result = querydf(
                db_file,
                "SELECT COUNT(*) as count FROM test_ro",
                readonly=true
            ) do conn
                # This function is executed with the connection in read-only mode
                # Any attempt to modify data should fail
                @test_throws Exception DuckDB.execute(conn, "UPDATE test_ro SET name = 'Updated' WHERE id = 1")

                # But we can create temporary views (these are session-only and don't modify the DB file)
                DuckDB.execute(conn, "CREATE TEMP VIEW filtered_view AS SELECT * FROM test_ro WHERE id > 2")
            end

            @test result[1, :count] == 4

            # Test with mixed sources
            df = DataFrame(id=[5, 6], name=["Eve", "Frank"])

            result = querydf(
                Dict("db" => db_file, "new_data" => df),
                """
                SELECT * FROM db.test_ro
                UNION ALL
                SELECT * FROM new_data
                ORDER BY id
                """,
                readonly=true
            )

            @test size(result) == (6, 2)
            @test result[5, :name] == "Eve"
            @test result[6, :name] == "Frank"

            # Try to modify database through mixed sources (should fail)
            @test_throws Exception querydf(
                Dict("db" => db_file, "new_data" => df),
                "INSERT INTO db.test_ro SELECT * FROM new_data",
                readonly=true
            )
        finally
            # Clean up the temporary database file
            if isfile(db_file)
                rm(db_file, force=true)
            end
        end
    end

    # Test that readonly is properly ignored for in-memory databases
    @testset "Read-only mode with in-memory database" begin
        # In-memory database should work the same with readonly=true or false
        # because it doesn't make sense to have a read-only in-memory database

        result1 = querydf(
            ":memory:",
            [
                "CREATE TABLE test_mem (id INTEGER, value DOUBLE)",
                "INSERT INTO test_mem VALUES (1, 10.5), (2, 20.5)",
                "SELECT * FROM test_mem ORDER BY id"
            ],
            readonly=false
        )

        result2 = querydf(
            ":memory:",
            [
                "CREATE TABLE test_mem (id INTEGER, value DOUBLE)",
                "INSERT INTO test_mem VALUES (1, 10.5), (2, 20.5)",
                "SELECT * FROM test_mem ORDER BY id"
            ],
            readonly=true  # This should be ignored for in-memory databases
        )

        @test size(result1) == size(result2)
        @test result1[!, :id] == result2[!, :id]
        @test result1[!, :value] == result2[!, :value]
    end

    # Test with edge cases
    @testset "Read-only mode edge cases" begin
        # Create a temporary database file
        db_file = tempname() * ".duckdb"

        try
            # Create an empty database
            querydf(db_file, "SELECT 1")

            # Test read-only on empty database
            result = querydf(
                db_file,
                "SELECT 1 as value",
                readonly=true
            )

            @test result[1, :value] == 1

            # Test with non-existent file (should fail with appropriate error)
            non_existent = tempname() * ".duckdb"
            @test_throws Exception querydf(
                non_existent,
                "SELECT 1",
                readonly=true
            )

            # Test with corrupt database file
            corrupt_file = tempname() * ".duckdb"
            open(corrupt_file, "w") do io
                write(io, "This is not a valid DuckDB file")
            end

            @test_throws Exception querydf(
                corrupt_file,
                "SELECT 1",
                readonly=true
            )
        finally
            # Clean up
            for file in [db_file, get(Main.Base.@locals, :corrupt_file, nothing)]
                if file !== nothing && isfile(file)
                    rm(file, force=true)
                end
            end
        end
    end

end
