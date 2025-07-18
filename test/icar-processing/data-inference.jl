using Test
using DataFrames
using FMDData
using Try

@testset verbose = true "Data Inference" begin
    @testset "infer_later_year_values - arithmetic correctness" begin
        # Create test data that mimics real ICAR structure
        initial_df = DataFrame(
            states_ut = ["Gujarat", "Maharashtra", "total"],
            serotype_o_count_pre = [100, 150, 250],
            serotype_o_count_post = [20, 30, 50],
            serotype_a_count_pre = [80, 120, 200],
            serotype_a_count_post = [16, 24, 40],
            serotype_all_count_pre = [180, 270, 450],
            serotype_all_count_post = [36, 54, 90]
        )

        # Cumulative data for later year (should be initial + later year values)
        cumulative_df = DataFrame(
            states_ut = ["Gujarat", "Maharashtra", "total"],
            serotype_o_count_pre = [150, 200, 350],  # +50, +50
            serotype_o_count_post = [35, 45, 80],    # +15, +15
            serotype_a_count_pre = [130, 170, 300],  # +50, +50
            serotype_a_count_post = [26, 34, 60],    # +10, +10
            serotype_all_count_pre = [280, 370, 650], # +100, +100
            serotype_all_count_post = [61, 79, 140]   # +25, +25
        )

        result = infer_later_year_values(cumulative_df, initial_df)
        @test Try.isok(result)

        inferred_df = Try.unwrap(result)

        # Test arithmetic correctness: cumulative - initial = inferred
        @test inferred_df[1, :serotype_o_count_pre] == 50  # 150 - 100
        @test inferred_df[2, :serotype_o_count_pre] == 50  # 200 - 150
        @test inferred_df[1, :serotype_o_count_post] == 15 # 35 - 20
        @test inferred_df[2, :serotype_o_count_post] == 15 # 45 - 30

        @test inferred_df[1, :serotype_a_count_pre] == 50  # 130 - 80
        @test inferred_df[2, :serotype_a_count_pre] == 50  # 170 - 120
        @test inferred_df[1, :serotype_a_count_post] == 10 # 26 - 16
        @test inferred_df[2, :serotype_a_count_post] == 10 # 34 - 24

        # Test that total rows are recalculated correctly
        total_row = inferred_df[inferred_df.states_ut .== "Total", :]
        @test nrow(total_row) == 1
        @test total_row[1, :serotype_o_count_pre] == 100  # Sum of Gujarat + Maharashtra
        @test total_row[1, :serotype_o_count_post] == 30
    end

    @testset "infer_later_year_values - missing value handling" begin
        initial_df = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [180, 270, 180 + 270],
            serotype_all_count_post = [36, 54, 36 + 54],
            serotype_o_count_pre = Union{Missing, Int}[100, missing, 100],
            serotype_o_count_post = [20, 30, 50]
        )

        cumulative_df = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [230, 320, 230 + 320],
            serotype_all_count_post = [51, 69, 51 + 69],
            serotype_o_count_pre = [150, 50, 200],  # State2: 50 - 0 (missing treated as 0)
            serotype_o_count_post = [35, 45, 80]
        )

        result = infer_later_year_values(cumulative_df, initial_df)
        @test Try.isok(result)

        inferred_df = Try.unwrap(result)
        @test inferred_df[1, :serotype_o_count_pre] == 50  # 150 - 100
        @test inferred_df[2, :serotype_o_count_pre] == 50  # 50 - 0 (missing treated as 0)
        @test inferred_df[1, :serotype_o_count_post] == 15 # 35 - 20
        @test inferred_df[2, :serotype_o_count_post] == 15 # 45 - 30
    end

    @testset "infer_later_year_values - error conditions" begin
        initial_df = DataFrame(
            states_ut = ["State1"],
            serotype_o_count_pre = [100]
        )

        # Test error when cumulative has missing value but initial doesn't
        cumulative_df_missing = DataFrame(
            states_ut = ["State1"],
            serotype_o_count_pre = Union{Missing, Int}[missing]
        )

        result = infer_later_year_values(cumulative_df_missing, initial_df)
        @test Try.iserr(result)
        @test contains(Try.unwrap_err(result), "value is missing in the follow-up dataset")
    end

    @testset "infer_later_year_values - rounding correction" begin
        # Test the -1 rounding correction feature
        initial_df = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [101, 100, 101]  # This will cause -1 after subtraction
        )

        cumulative_df = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [100, 150, 100]  # 100 - 101 = -1, should be corrected to 0
        )

        result = infer_later_year_values(cumulative_df, initial_df)
        @test Try.isok(result)

        inferred_df = Try.unwrap(result)
        @test inferred_df.states_ut == ["State2", "Total"]  # Because State1 observed no additional samples, remove them from the inferred table
    end

    @testset "infer_later_year_values - state matching" begin
        # Test with different states between dataframes
        initial_df = DataFrame(
            states_ut = ["State1", "State2", "State3", "Total"],
            serotype_all_count_pre = [100, 200, 300, 600]
        )

        cumulative_df = DataFrame(
            states_ut = ["State1", "State3", "Total"],  # State2 missing
            serotype_all_count_pre = [150, 350, 500]
        )

        result = infer_later_year_values(cumulative_df, initial_df)
        @test Try.isok(result)

        inferred_df = Try.unwrap(result)
        # Only State1 and State3 should be processed (common states)
        @test "State1" in inferred_df.states_ut
        @test "State3" in inferred_df.states_ut
        @test inferred_df[inferred_df.states_ut .== "State1", :serotype_all_count_pre][1] == 50  # 150 - 100
        @test inferred_df[inferred_df.states_ut .== "State3", :serotype_all_count_pre][1] == 50  # 350 - 300
    end

    @testset "infer_later_year_values - extreme missing value scenarios" begin
        # Test with initial DataFrame having all missing values
        all_missing_initial = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = Union{Missing, Int}[missing, missing, missing],
            serotype_all_count_post = Union{Missing, Int}[missing, missing, missing],
            serotype_o_count_pre = Union{Missing, Int}[missing, missing, missing],
            serotype_o_count_post = Union{Missing, Int}[missing, missing, missing]
        )

        normal_cumulative = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [180, 270, 450],
            serotype_all_count_post = [36, 54, 90],
            serotype_o_count_pre = [100, 150, 250],
            serotype_o_count_post = [20, 30, 50]
        )

        result = infer_later_year_values(normal_cumulative, all_missing_initial)
        @test Try.isok(result)

        inferred_df = Try.unwrap(result)
        # Missing values in initial should be treated as 0
        @test inferred_df[1, :serotype_o_count_pre] == 100
        @test inferred_df[2, :serotype_o_count_pre] == 150

        # Create expected DataFrame with the percentage columns that get added
        # Note: Column order must match the actual output from infer_later_year_values
        expected_df = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [180, 270, 450],
            serotype_all_count_post = [36, 54, 90],
            serotype_o_count_pre = [100, 150, 250],
            serotype_o_pct_pre = [55.6, 55.6, 55.6],
            serotype_o_count_post = [20, 30, 50],
            serotype_o_pct_post = [55.6, 55.6, 55.6]
        )
        @test inferred_df == expected_df

        # Test with cumulative DataFrame having mixed missing values
        mixed_missing_cumulative = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [180, 270, 450],
            serotype_all_count_post = [36, 54, 90],
            serotype_o_count_pre = Union{Missing, Int}[100, missing, 100],
            serotype_o_count_post = [20, 30, 50]
        )

        normal_initial = DataFrame(
            states_ut = ["State1", "State2", "Total"],
            serotype_all_count_pre = [90, 135, 225],
            serotype_all_count_post = [18, 27, 45],
            serotype_o_count_pre = [50, 75, 125],
            serotype_o_count_post = [10, 15, 25]
        )

        result_mixed = infer_later_year_values(mixed_missing_cumulative, normal_initial)
        @test Try.iserr(result_mixed)  # Should error when cumulative missing but initial not
    end
end
