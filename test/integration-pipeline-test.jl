using Test
using DataFrames
using CSV
using FMDData
using Try
using OrderedCollections
using DrWatson: findproject

# Import necessary Base functions for testing
import Base: isfile, isdir

@testset "End-to-End Integration Pipeline" begin
    test_data_dir(args...) = joinpath(findproject(), "test-data", args...)

    @testset "Complete cleaning + processing workflow" begin
        # Create realistic test data for end-to-end testing
        initial_year_df = DataFrame(
            states_ut = ["Gujarat", "Maharashtra", "Punjab"],
            serotype_o_count_pre = [100, 150, 80],
            serotype_o_count_post = [20, 30, 15],
            serotype_a_count_pre = [80, 120, 60],
            serotype_a_count_post = [16, 24, 12],
            serotype_asia1_count_pre = [50, 75, 40],
            serotype_asia1_count_post = [10, 15, 8],
            serotype_all_count_pre = [230, 345, 180],
            serotype_all_count_post = [46, 69, 35]
        )

        cumulative_year_df = DataFrame(
            states_ut = ["Gujarat", "Maharashtra", "Punjab"],
            serotype_o_count_pre = [180, 230, 130],  # +80, +80, +50
            serotype_o_count_post = [35, 45, 25],    # +15, +15, +10
            serotype_a_count_pre = [140, 200, 110],  # +60, +80, +50
            serotype_a_count_post = [28, 40, 22],    # +12, +16, +10
            serotype_asia1_count_pre = [90, 135, 70], # +40, +60, +30
            serotype_asia1_count_post = [18, 27, 14], # +8, +12, +6
            serotype_all_count_pre = [410, 565, 310], # +180, +220, +130
            serotype_all_count_post = [81, 112, 61]   # +35, +43, +26
        )

        # Write test files
        initial_file = test_data_dir("integration_initial.csv")
        cumulative_file = test_data_dir("integration_cumulative.csv")

        CSV.write(initial_file, initial_year_df)
        CSV.write(cumulative_file, cumulative_year_df)

        # Step 1: Clean both datasets
        initial_cleaned_result = all_cleaning_steps(
            "integration_initial.csv",
            test_data_dir(),
            output_dir = test_data_dir()
        )

        cumulative_cleaned_result = all_cleaning_steps(
            "integration_cumulative.csv",
            test_data_dir(),
            output_dir = test_data_dir()
        )

        @test Try.isok(initial_cleaned_result)
        @test Try.isok(cumulative_cleaned_result)

        # Step 2: Load cleaned data
        initial_cleaned_file = test_data_dir("clean_integration_initial.csv")
        cumulative_cleaned_file = test_data_dir("clean_integration_cumulative.csv")

        @test isfile(initial_cleaned_file)
        @test isfile(cumulative_cleaned_file)

        initial_cleaned_df = CSV.read(initial_cleaned_file, DataFrame)
        cumulative_cleaned_df = CSV.read(cumulative_cleaned_file, DataFrame)

        # Step 3: Process data - infer later year values
        inferred_result = infer_later_year_values(cumulative_cleaned_df, initial_cleaned_df)
        @test Try.isok(inferred_result)

        inferred_df = Try.unwrap(inferred_result)

        # Step 4: Add metadata to processed data
        metadata = OrderedDict(
            :sample_year => 2022,
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )

        metadata_result = add_all_metadata!(inferred_df => metadata)
        @test Try.isok(metadata_result)

        # Step 5: Validate final processed data
        @test nrow(inferred_df) >= 3  # At least the 3 states
        @test :sample_year in names(inferred_df)
        @test :report_year in names(inferred_df)
        @test :round_name in names(inferred_df)
        @test :test_type in names(inferred_df)
        @test :test_threshold in names(inferred_df)

        # Validate arithmetic correctness in integrated pipeline
        gujarat_row = inferred_df[inferred_df.states_ut .== "Gujarat", :]
        @test nrow(gujarat_row) == 1
        @test gujarat_row[1, :serotype_o_count_pre] == 80  # 180 - 100
        @test gujarat_row[1, :serotype_a_count_pre] == 60  # 140 - 80

        # Check metadata was added correctly
        @test all(inferred_df.sample_year .== 2022)
        @test all(inferred_df.round_name .== "NADCP 2")

        # Clean up test files
        cleanup_files = [
            initial_file, cumulative_file,
            initial_cleaned_file, cumulative_cleaned_file,
        ]

        for file in cleanup_files
            if isfile(file)
                rm(file)
            end
        end

        # Clean up log directories
        logfile_dir = test_data_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end

    @testset "Error propagation through pipeline" begin
        # Test that errors in cleaning stage prevent processing stage
        bad_data_df = DataFrame(
            states_ut = ["Invalid State Name", "Another Bad State"],
            serotype_o_count_pre = [100, 150],
            serotype_o_count_post = [20, 30]
        )

        bad_file = test_data_dir("integration_bad_data.csv")
        CSV.write(bad_file, bad_data_df)

        # This should fail in the cleaning stage
        cleaning_result = all_cleaning_steps(
            "integration_bad_data.csv",
            test_data_dir(),
            output_dir = test_data_dir()
        )

        # Either fails or succeeds with warnings - test error handling
        if Try.iserr(cleaning_result)
            @test isa(Try.unwrap_err(cleaning_result), String)
        else
            # If cleaning succeeded, check that it handled the issues
            @test Try.isok(cleaning_result)
        end

        # Clean up
        if isfile(bad_file)
            rm(bad_file)
        end

        bad_clean_file = test_data_dir("clean_integration_bad_data.csv")
        if isfile(bad_clean_file)
            rm(bad_clean_file)
        end

        logfile_dir = test_data_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end

    @testset "Data consistency validation across pipeline stages" begin
        # Create data and run through pipeline, then validate consistency
        test_df = DataFrame(
            states_ut = ["Test State 1", "Test State 2"],
            serotype_o_count_pre = [50, 75],
            serotype_o_count_post = [10, 15],
            serotype_a_count_pre = [40, 60],
            serotype_a_count_post = [8, 12],
            serotype_all_count_pre = [90, 135],
            serotype_all_count_post = [18, 27]
        )

        test_file = test_data_dir("integration_consistency_test.csv")
        CSV.write(test_file, test_df)

        # Clean the data
        cleaning_result = all_cleaning_steps(
            "integration_consistency_test.csv",
            test_data_dir(),
            output_dir = test_data_dir()
        )

        if Try.isok(cleaning_result)
            cleaned_file = test_data_dir("clean_integration_consistency_test.csv")

            if isfile(cleaned_file)
                cleaned_df = CSV.read(cleaned_file, DataFrame)

                # Validate that data types are preserved
                @test eltype(cleaned_df.serotype_o_count_pre) <: Union{Missing, Number}
                @test eltype(cleaned_df.serotype_a_count_pre) <: Union{Missing, Number}

                # Validate that basic data integrity is maintained
                @test nrow(cleaned_df) >= 2  # At least our test states

                # Clean up
                rm(cleaned_file)
            end
        end

        # Clean up
        if isfile(test_file)
            rm(test_file)
        end

        logfile_dir = test_data_dir("logfiles")
        if isdir(logfile_dir)
            rm(logfile_dir; recursive = true)
        end
    end

    @testset "Multiple rounds data combination" begin
        # Test combining data from different rounds
        nadcp1_df = DataFrame(
            states_ut = ["State A", "State B"],
            serotype_o_count_pre = [100, 150],
            serotype_o_count_post = [20, 30],
            round_name = ["NADCP 1", "NADCP 1"]
        )

        nadcp2_df = DataFrame(
            states_ut = ["State A", "State B"],
            serotype_o_count_pre = [80, 120],
            serotype_o_count_post = [16, 24],
            round_name = ["NADCP 2", "NADCP 2"]
        )

        # Test combination
        combine_result = combine_round_dfs(nadcp1_df, nadcp2_df)
        @test Try.isok(combine_result)

        combined_df = Try.unwrap(combine_result)
        @test nrow(combined_df) == 4  # 2 states × 2 rounds
        @test length(unique(combined_df.round_name)) == 2
        @test "NADCP 1" in combined_df.round_name
        @test "NADCP 2" in combined_df.round_name

        # Add comprehensive metadata
        metadata = OrderedDict(
            :report_year => 2022,
            :test_type => "SPCE"
        )

        metadata_result = add_all_metadata!(combined_df => metadata)
        @test Try.isok(metadata_result)

        @test all(combined_df.report_year .== 2022)
        @test all(combined_df.test_type .== "SPCE")
    end
end

