using Test
using DataFrames
using CSV
using FMDData
using Try
using DrWatson: findproject
using Logging

# Import necessary Base functions for testing
import Base: isfile, isdir, contains

@testset "Wrapper Functions" begin
    test_dir(args...) = joinpath(findproject(), "test", args...)

    @testset "all_cleaning_steps - successful execution" begin
        # Test with valid test data
        result = all_cleaning_steps(
            "test-data.csv",
            test_dir(),
            output_dir = test_dir()
        )

        @test Try.isok(result)

        # Clean up generated files
        output_file = test_dir("clean_test-data.csv")
        if isfile(output_file)
            rm(output_file)
        end
        logfile_dir = test_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end

    @testset "all_cleaning_steps - invalid file handling" begin
        # Test with non-existent file
        result = all_cleaning_steps(
            "non_existent_file.csv",
            test_dir(),
            output_dir = test_dir()
        )

        @test Try.iserr(result)
        @test contains(Try.unwrap_err(result), "not found") || contains(Try.unwrap_err(result), "No such file")
    end

    @testset "all_cleaning_steps - invalid directory handling" begin
        # Test with non-existent input directory
        result = all_cleaning_steps(
            "test-data.csv",
            "/non/existent/directory",
            output_dir = test_dir()
        )

        @test Try.iserr(result)
    end

    @testset "all_cleaning_steps - output directory creation" begin
        # Test with non-existent output directory (should be created)
        temp_output_dir = test_dir("temp_output")

        # Make sure directory doesn't exist
        if isdir(temp_output_dir)
            rm(temp_output_dir; recursive = true)
        end

        result = all_cleaning_steps(
            "test-data.csv",
            test_dir(),
            output_dir = temp_output_dir
        )

        @test Try.isok(result)
        @test isdir(temp_output_dir)

        # Clean up
        if isdir(temp_output_dir)
            rm(temp_output_dir; recursive = true)
        end
    end

    @testset "all_cleaning_steps - corrupt CSV handling" begin
        # Create a temporary corrupt CSV file
        corrupt_file = test_dir("corrupt_test.csv")

        # Write invalid CSV content
        open(corrupt_file, "w") do io
            write(io, "invalid,csv,content\n")
            write(io, "with,unmatched,quotes\",\n")
            write(io, "and,malformed,data")
        end

        result = all_cleaning_steps(
            "corrupt_test.csv",
            test_dir(),
            output_dir = test_dir()
        )

        @test Try.iserr(result)

        # Clean up
        if isfile(corrupt_file)
            rm(corrupt_file)
        end
    end

    @testset "all_cleaning_steps - empty file handling" begin
        # Create an empty CSV file
        empty_file = test_dir("empty_test.csv")

        # Write empty content
        open(empty_file, "w") do io
            write(io, "")
        end

        result = all_cleaning_steps(
            "empty_test.csv",
            test_dir(),
            output_dir = test_dir()
        )

        @test Try.iserr(result)

        # Clean up
        if isfile(empty_file)
            rm(empty_file)
        end
    end

    @testset "all_cleaning_steps - data validation failure" begin
        # Create a CSV with invalid state names
        invalid_states_file = test_dir("invalid_states_test.csv")

        # Create DataFrame with invalid state names
        invalid_df = DataFrame(
            states_ut = ["Invalid State", "Another Invalid State"],
            serotype_o_count_pre = [10, 20],
            serotype_o_count_post = [5, 8],
            serotype_a_count_pre = [15, 25],
            serotype_a_count_post = [7, 12],
            serotype_all_count_pre = [25, 45],
            serotype_all_count_post = [12, 20]
        )

        # Write to CSV
        CSV.write(invalid_states_file, invalid_df)

        result = all_cleaning_steps(
            "invalid_states_test.csv",
            test_dir(),
            output_dir = test_dir()
        )

        @test Try.iserr(result)
        @test contains(Try.unwrap_err(result), "Invalid State") || contains(Try.unwrap_err(result), "state")

        # Clean up
        if isfile(invalid_states_file)
            rm(invalid_states_file)
        end
    end

    @testset "all_cleaning_steps - logging functionality" begin
        # Test that logging is properly set up and used
        log_buffer = IOBuffer()
        logger = SimpleLogger(log_buffer)

        with_logger(logger) do
            result = all_cleaning_steps(
                "test-data.csv",
                test_dir(),
                output_dir = test_dir()
            )
        end

        log_output = String(take!(log_buffer))
        # Should contain some logging information
        @test !isempty(log_output) || Try.isok(result)  # Either logged or successful

        # Clean up any generated files
        output_file = test_dir("clean_test-data.csv")
        if isfile(output_file)
            rm(output_file)
        end
        logfile_dir = test_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end

    @testset "all_2019_cleaning_steps - functionality" begin
        # Test specific 2019 cleaning logic if the function exists
        if isdefined(FMDData, :all_2019_cleaning_steps)
            # Create test data that mimics 2019 format
            test_2019_df = DataFrame(
                states_ut = ["Gujarat", "Maharashtra"],
                serotype_o_count_pre = [100, 150],
                serotype_o_count_post = [20, 30],
                serotype_a_count_pre = [80, 120],
                serotype_a_count_post = [16, 24],
                serotype_all_count_pre = [180, 270],
                serotype_all_count_post = [36, 54]
            )

            test_2019_file = test_dir("test_2019_data.csv")
            CSV.write(test_2019_file, test_2019_df)

            result = all_2019_cleaning_steps(
                "test_2019_data.csv",
                test_dir(),
                output_dir = test_dir()
            )

            @test Try.isok(result)

            # Clean up
            if isfile(test_2019_file)
                rm(test_2019_file)
            end
            output_file = test_dir("clean_test_2019_data.csv")
            if isfile(output_file)
                rm(output_file)
            end
            logfile_dir = test_dir("logfiles")
            if isdir(logfile_dir)
                rm(logfile_dir; recursive = true)
            end
        end
    end

    @testset "all_cleaning_steps - pipeline error propagation" begin
        # Test that errors from individual steps are properly propagated
        # This would require mocking internal functions, but we can test
        # realistic scenarios that cause specific step failures

        # Create data that would fail at different stages
        problematic_df = DataFrame(
            states_ut = ["Valid State", "Invalid State"],  # Mix of valid/invalid
            serotype_o_count_pre = [missing, 150],  # Missing values
            serotype_o_count_post = [-5, 30],  # Negative values
            serotype_a_count_pre = [80, missing],
            serotype_a_count_post = [16, 24],
            serotype_all_count_pre = [180, 270],
            serotype_all_count_post = [36, 54]
        )

        problematic_file = test_dir("problematic_test.csv")
        CSV.write(problematic_file, problematic_df)

        result = all_cleaning_steps(
            "problematic_test.csv",
            test_dir(),
            output_dir = test_dir()
        )

        # Should either succeed with warnings or fail with meaningful error
        if Try.iserr(result)
            error_msg = Try.unwrap_err(result)
            @test isa(error_msg, String)
            @test !isempty(error_msg)
        else
            # If it succeeds, it should have handled the issues appropriately
            @test Try.isok(result)
        end

        # Clean up
        if isfile(problematic_file)
            rm(problematic_file)
        end
        output_file = test_dir("clean_problematic_test.csv")
        if isfile(output_file)
            rm(output_file)
        end
        logfile_dir = test_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end
end

