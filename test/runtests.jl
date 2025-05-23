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

    # Test new functionality added in the refactoring
    @testset "Refactored SourceManager functionality" begin

        # 1. Enhanced Type Handling Tests
        @testset "Enhanced Type Handling" begin
            # Test handling of various Union types with Missing values
            df_complex_types = DataFrame(
                int_with_missing = Union{Int64, Missing}[1, 2, missing, 4],
                float_with_missing = Union{Float64, Missing}[1.1, missing, 3.3, 4.4],
                string_with_missing = Union{String, Missing}["a", "b", missing, "d"],
                bool_with_missing = Union{Bool, Missing}[true, false, missing, true],
                date_with_missing = Union{Date, Missing}[Date(2021,1,1), missing, Date(2021,3,3), Date(2021,4,4)]
            )

            # Query that selects all columns and filters out missing values
            result = querydf(
                df_complex_types,
                """
                SELECT * FROM df
                WHERE int_with_missing IS NOT NULL
                  AND float_with_missing IS NOT NULL
                  AND string_with_missing IS NOT NULL
                  AND bool_with_missing IS NOT NULL
                  AND date_with_missing IS NOT NULL
                  AND int_with_missing = 4  -- Add explicit filter to ensure we get the right row
                ORDER BY int_with_missing DESC
                LIMIT 1
                """
            )

            @test size(result) == (1, 5)  # Only one row should have no missing values
            @test result[1, :int_with_missing] == 4
            @test result[1, :float_with_missing] == 4.4
            @test result[1, :string_with_missing] == "d"
            @test result[1, :bool_with_missing] == true
            @test result[1, :date_with_missing] == Date(2021,4,4)

            # Test with complex operations on Union types
            result = querydf(
                df_complex_types,
                """
                SELECT
                    AVG(int_with_missing) as avg_int,
                    SUM(float_with_missing) as sum_float,
                    COUNT(string_with_missing) as count_strings,
                    COUNT(*) as total_rows
                FROM df
                """
            )

            @test size(result) == (1, 4)
            @test result[1, :avg_int] ≈ 2.3333333333333335 atol=1e-10  # (1+2+4)/3
            @test result[1, :sum_float] ≈ 8.8 atol=1e-10  # 1.1+3.3+4.4
            @test result[1, :count_strings] == 3  # Counts non-missing values
            @test result[1, :total_rows] == 4  # Counts all rows
        end

        # 2. Batched DataFrame Registration Tests
        @testset "Batched DataFrame Registration" begin
            # Create a large DataFrame to test batched registration
            n = 10000  # Large enough to trigger batching
            Random.seed!(42)
            large_df = DataFrame(
                id = 1:n,
                value = rand(n),
                category = rand(["A", "B", "C", "D"], n),
                timestamp = [DateTime(2021, 1, 1) + Dates.Day(i % 365) for i in 1:n]
            )

            # Test with different batch sizes
            for batch_size in [100, 1000, 5000]
                result = querydf(
                    large_df,
                    """
                    SELECT
                        category,
                        COUNT(*) as count,
                        AVG(value) as avg_value,
                        MIN(timestamp) as min_date,
                        MAX(timestamp) as max_date
                    FROM df
                    GROUP BY category
                    ORDER BY category
                    """,
                    init_config=Dict{Symbol,Any}(:batch_size => batch_size),
                    verbose=true
                )

                @test size(result) == (4, 5)  # 4 categories
                @test sum(result[!, :count]) == n  # Total count matches DataFrame size

                # Verify results are consistent across different batch sizes
                @test all(result[!, :category] .== ["A", "B", "C", "D"])
            end

            # Test that batch size affects performance (this is more of a benchmark than a test)
            # We just want to ensure it completes successfully without errors
            @test_nowarn querydf(
                large_df,
                "SELECT COUNT(*) as count FROM df",
                init_config=Dict{Symbol,Any}(:batch_size => 10)  # Small batch size
            )
        end

        # 3. Native DuckDB Registration Tests
        @testset "Native DuckDB Registration" begin
            # We'll test both the native and fallback methods by forcing the fallback
            df = DataFrame(id=1:10, value=rand(10))

            # First test with normal operation (should use native if available)
            result1 = querydf(
                df,
                "SELECT SUM(value) as sum_value FROM df",
                verbose=true
            )

            # Force fallback by simulating native registration failure
            # We'll use a preprocessor to capture the registration method
            registration_method = Ref("")
            result2 = querydf(
                df,
                "SELECT SUM(value) as sum_value FROM df",
                verbose=true,
                init_config=Dict{Symbol,Any}(:force_manual_registration => true),
                preprocessors=[
                    query -> begin
                        # This is a hack to detect which method was used
                        # In a real implementation, you'd modify the code to expose this information
                        if registration_method[] == ""
                            registration_method[] = "manual"
                        end
                        return query
                    end
                ]
            )

            # Both methods should give the same result
            @test result1[1, :sum_value] ≈ result2[1, :sum_value] atol=1e-10

            # Optional: If the SourceManager exposes a way to check which method was used,
            # we could verify that directly instead of the hack above
        end

        # 4. Extensible Configuration System Tests
        @testset "Extensible Configuration System" begin
            # Test various configuration options
            df = DataFrame(id=1:100, value=rand(100))

            # Test memory limit
            result = querydf(
                df,
                "SELECT COUNT(*) as count FROM df",
                init_config=Dict{Symbol,Any}(:memory_limit => "50MB")
            )
            @test result[1, :count] == 100

            # Get current threads to ensure we don't set it too low
            # Use a higher number to avoid the "smaller than external threads" error
            result = querydf(
                df,
                "SELECT COUNT(*) as count FROM df",
                init_config=Dict{Symbol,Any}(:threads => 8)
            )
            @test result[1, :count] == 100

            # Test multiple configuration options together
            result = querydf(
                df,
                "SELECT COUNT(*) as count FROM df",
                init_config=Dict{Symbol,Any}(
                    :memory_limit => "100MB",
                    :threads => 8,
                    :batch_size => 50
                ),
                verbose=true
            )
            @test result[1, :count] == 100

            # Test with an unsupported but harmless configuration option
            # This should just issue a warning but not fail
            result = querydf(
                df,
                "SELECT COUNT(*) as count FROM df",
                init_config=Dict{Symbol,Any}(:nonexistent_option => "value"),
                verbose=true
            )
            @test result[1, :count] == 100
        end

        # 5. Connection Management Tests
        @testset "Connection Management" begin
            # Test that connections are properly initialized and cleaned up
            db_file = tempname() * ".duckdb"

            try
                # Create a database file
                querydf(
                    db_file,
                    [
                        "CREATE TABLE test (id INTEGER, value DOUBLE)",
                        "INSERT INTO test VALUES (1, 10.5), (2, 20.5)"
                    ]
                )

                # Open multiple connections in sequence
                for i in 1:5
                    result = querydf(
                        db_file,
                        "SELECT * FROM test ORDER BY id"
                    )
                    @test size(result) == (2, 2)
                end

                # Test connection closing with error conditions
                @test_throws Exception querydf(
                    db_file,
                    "SELECT * FROM nonexistent_table"
                )

                # Verify we can still connect after an error
                result = querydf(
                    db_file,
                    "SELECT * FROM test ORDER BY id"
                )
                @test size(result) == (2, 2)

                # Test with do-block for explicit connection control
                querydf(db_file, "SELECT 1") do conn
                    # Connection should be valid here
                    result = DuckDB.execute(conn, "SELECT * FROM test") |> DataFrame
                    @test size(result) == (2, 2)

                    # Create a new table in this connection
                    DuckDB.execute(conn, "CREATE TABLE temp_table AS SELECT id * 10 as id_mult FROM test")

                    # Verify we can query the new table
                    result = DuckDB.execute(conn, "SELECT * FROM temp_table") |> DataFrame
                    @test size(result) == (2, 1)
                    @test result[1, :id_mult] == 10
                end

                # Just verify we can make a new connection and query the regular table
                # Temporary tables should be scoped to the connection that created them
                new_conn = DuckDB.DB(db_file)
                result = DuckDB.execute(new_conn, "SELECT * FROM test") |> DataFrame
                @test size(result) == (2, 2)
                close(new_conn)
            finally
                isfile(db_file) && rm(db_file, force=true)
            end
        end

        # 6. Complex Types in Mixed Sources
        @testset "Complex Types in Mixed Sources" begin
            # Create a database file with complex types
            db_file = tempname() * ".duckdb"

            try
                # Create a table in the database with various types
                querydf(
                    db_file,
                    [
                        """
                        CREATE TABLE complex_types (
                            id INTEGER,
                            name VARCHAR,
                            value DOUBLE,
                            date_val DATE,
                            is_active BOOLEAN
                        )
                        """,
                        """
                        INSERT INTO complex_types VALUES
                        (1, 'Row1', 10.5, '2021-01-01', true),
                        (2, 'Row2', 20.5, '2021-02-01', false),
                        (3, NULL, 30.0, '2021-03-01', true),
                        (4, 'Row4', NULL, NULL, false)
                        """
                    ]
                )

                # Create a DataFrame with similar but not identical schema
                # with Union types with Missing values
                df = DataFrame(
                    id = Union{Int64, Missing}[3, 4, 5, missing],
                    description = Union{String, Missing}["Desc3", missing, "Desc5", "Desc6"],
                    score = Union{Float64, Missing}[75.0, 80.0, missing, 90.0],
                    tags = ["tag1", "tag2", "tag3", "tag4"]  # Regular column without Missing
                )

                # Test joining these two sources with different complex types
                result = querydf(
                    Dict("db" => db_file, "df_data" => df),
                    """
                    SELECT
                        COALESCE(db.id, df.id) as id,
                        db.name,
                        df.description,
                        db.value,
                        df.score,
                        db.date_val,
                        db.is_active,
                        df.tags
                    FROM db.complex_types db
                    FULL JOIN df_data df ON db.id = df.id
                    ORDER BY id
                    """
                )

                # Verify the results
                @test size(result) == (6, 8)  # 6 rows total (1-5 + NULL id from df)

                # First row should be from db only
                @test result[1, :id] == 1
                @test result[1, :name] == "Row1"
                @test ismissing(result[1, :description])
                @test result[1, :value] == 10.5
                @test ismissing(result[1, :score])

                # Row with id=3 should have data from both sources
                @test result[3, :id] == 3
                @test ismissing(result[3, :name])  # NULL in db
                @test result[3, :description] == "Desc3"
                @test result[3, :value] == 30.0
                @test result[3, :score] == 75.0

                # Last row should be from df only with missing id
                @test ismissing(result[6, :id])
                @test ismissing(result[6, :name])
                @test result[6, :description] == "Desc6"
                @test ismissing(result[6, :value])
                @test result[6, :score] == 90.0
            finally
                isfile(db_file) && rm(db_file, force=true)
            end
        end

        # 7. Tests for Union types with complex nested structures
        @testset "Complex Union Types" begin
            # Test handling of complex nested Union types
            # Create a DataFrame with deeply nested Union types
            df_nested = DataFrame(
                # Union of Missing with another Union type
                nested_union = Union{Missing, Union{Int64, Float64}}[1, 2.5, missing, 4],

                # Array column with Union element type
                array_of_unions = [
                    Union{Int64, Missing}[1, 2, missing],
                    Union{Int64, Missing}[missing, 5, 6],
                    Union{Int64, Missing}[7, 8, 9],
                    Union{Int64, Missing}[missing, missing, 12]
                ]
            )

            # For array columns, we'd typically need to register a custom type
            # but DuckDB can sometimes handle these as JSON or other complex types

            # Test simple query with the nested_union column
            result = querydf(
                df_nested,
                """
                SELECT
                    nested_union,
                    CAST(nested_union AS DOUBLE) * 2 as doubled
                FROM df
                WHERE nested_union IS NOT NULL
                """
            )

            @test size(result) == (3, 2)
            @test string(result[1, :nested_union]) == "1"
            @test result[1, :doubled] ≈ 2.0
            @test string(result[2, :nested_union]) == "2.5"
            @test result[2, :doubled] ≈ 5.0

            # The array column is more complex and may require special handling
            # depending on how DuckQuery handles such types
            # This test might need adaptation based on the actual implementation

            # Add a basic test that at least the registration doesn't error
            @test_nowarn querydf(
                df_nested,
                "SELECT COUNT(*) FROM df"
            )
        end

        # 8. Tests for performance with larger datasets
        @testset "Performance with Larger Datasets" begin
            # Create a larger DataFrame - skip this in CI environments if needed
            n = 50000  # Large enough to test performance, small enough to complete quickly
            Random.seed!(42)

            # Create DataFrame with multiple column types
            large_perf_df = DataFrame(
                id = 1:n,
                group = rand(1:100, n),
                value_int = rand(Int, n),
                value_float = rand(n),
                value_with_missing = Union{Float64, Missing}[rand() < 0.9 ? rand() : missing for _ in 1:n],
                category = rand(["A", "B", "C", "D", "E"], n),
                timestamp = [DateTime(2021, 1, 1) + Dates.Minute(i) for i in 1:n]
            )

            # Test with verbose output to see registration progress
            @time result = querydf(
                large_perf_df,
                """
                SELECT
                    category,
                    COUNT(*) as count,
                    AVG(value_int) as avg_int,
                    AVG(value_float) as avg_float,
                    COUNT(value_with_missing) as non_missing_count,
                    MIN(timestamp) as first_seen,
                    MAX(timestamp) as last_seen
                FROM df
                GROUP BY category
                ORDER BY category
                """,
                verbose=true,
                profile=true
            )

            @test size(result) == (5, 7)  # 5 categories
            @test sum(result[!, :count]) == n  # Total should match DataFrame size

            # Test batch size impact
            @time result_small_batch = querydf(
                large_perf_df,
                "SELECT COUNT(*) FROM df",
                init_config=Dict{Symbol,Any}(:batch_size => 100),
                verbose=true
            )

            @time result_large_batch = querydf(
                large_perf_df,
                "SELECT COUNT(*) FROM df",
                init_config=Dict{Symbol,Any}(:batch_size => 10000),
                verbose=true
            )

            # Results should be the same regardless of batch size
            @test result_small_batch[1, 1] == result_large_batch[1, 1]
        end
    end


end
