using DuckQuery
using DataFrames
using Test

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
        df = DataFrame(id = [1, 2, 3], name = ["Alice", "Bob", "Charlie"])
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
        customers = DataFrame(id = [1, 2, 3], name = ["Alice", "Bob", "Charlie"])
        orders = DataFrame(id = [101, 102], customer_id = [1, 3], amount = [100, 200])
        
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
        df = DataFrame(id = [1, 2, 3], age = [25, 17, 30])
        
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
                id = [1, 2, 3, 4, 5],
                name = ["Alice", "Bob", "Charlie", "David", "Eve"],
                score = [95.5, 87.2, 92.0, 78.5, 88.9],
                active = [true, false, true, true, false]
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
                init_config=Dict{Symbol, Any}(:memory_limit => "100MB"),
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
            @test result[!, :score] ≈ original_df[!, :score] atol=1e-5
            @test result[!, :active] == original_df[!, :active]
            
            # Test a more complex query on the database file
            filtered_result = querydf(
                db_file,
                "SELECT name, score FROM users WHERE active = true ORDER BY score DESC"
            )
            
            @test size(filtered_result) == (3, 2)
            # Sort by score in descending order, so Alice (95.5) is first, then Charlie (92.0), then David (78.5)
            @test filtered_result[1, :name] == "Alice"
            @test filtered_result[1, :score] ≈ 95.5 atol=1e-5
            @test filtered_result[2, :name] == "Charlie"
            @test filtered_result[3, :name] == "David"
        finally
            # Clean up the temporary database file
            if isfile(db_file)
                rm(db_file, force=true)
            end
        end
    end
end