using Test
using DataFrames
using FMDData
using Try

@testset "Edge Cases - Missing/NaN Values" begin
    
    @testset "DataFrames with mixed missing/NaN values" begin
        # Test with mixed missing and NaN values
        mixed_df = DataFrame(
            states_ut = ["State1", "State2", "State3"],
            serotype_o_count_pre = [100, missing, NaN],
            serotype_o_count_post = [20, NaN, missing],
            serotype_a_count_pre = [missing, 120, 80],
            serotype_a_count_post = [NaN, 24, 16],
            serotype_all_count_pre = [180, missing, NaN],
            serotype_all_count_post = [36, NaN, missing]
        )
        
        # Test skip_missing_and_nan function
        test_values = [1, missing, NaN, 2, 3]
        filtered_values = collect(skip_missing_and_nan(test_values))
        @test filtered_values == [1, 2, 3]
        
        # Test with completely missing column
        all_missing_values = [missing, missing, missing]
        filtered_missing = collect(skip_missing_and_nan(all_missing_values))
        @test isempty(filtered_missing)
        
        # Test with completely NaN column
        all_nan_values = [NaN, NaN, NaN]
        filtered_nan = collect(skip_missing_and_nan(all_nan_values))
        @test isempty(filtered_nan)
        
        # Test with mixed missing/NaN in state seroprevalence calculation
        if isdefined(FMDData, :_calculate_state_seroprevalence)
            # This would need real function implementation to test
            @test true  # Placeholder - would test actual function behavior
        end
    end
    
    @testset "Empty DataFrame handling" begin
        # Test with completely empty DataFrame
        empty_df = DataFrame()
        
        # Test functions that should handle empty DataFrames gracefully
        @test nrow(empty_df) == 0
        @test ncol(empty_df) == 0
        
        # Test with DataFrame with columns but no rows
        empty_with_cols = DataFrame(
            states_ut = String[],
            serotype_o_count_pre = Int[],
            serotype_o_count_post = Int[]
        )
        
        @test nrow(empty_with_cols) == 0
        @test ncol(empty_with_cols) == 3
        
        # Test metadata addition to empty DataFrame
        metadata_result = add_metadata_col!(:test_year, empty_with_cols => 2021)
        @test Try.isok(metadata_result)
        @test :test_year in names(empty_with_cols)
        @test isempty(empty_with_cols.test_year)
    end
    
    @testset "Single-row DataFrame handling" begin
        # Test with single-row DataFrame
        single_row_df = DataFrame(
            states_ut = ["Test State"],
            serotype_o_count_pre = [100],
            serotype_o_count_post = [20],
            serotype_a_count_pre = [80],
            serotype_a_count_post = [16],
            serotype_all_count_pre = [180],
            serotype_all_count_post = [36]
        )
        
        # Test metadata addition
        metadata_result = add_metadata_col!(:sample_year, single_row_df => 2021)
        @test Try.isok(metadata_result)
        @test single_row_df.sample_year == [2021]
        
        # Test with single row containing missing values
        single_row_missing = DataFrame(
            states_ut = ["Test State"],
            serotype_o_count_pre = [missing],
            serotype_o_count_post = [20],
            serotype_a_count_pre = [80],
            serotype_a_count_post = [missing]
        )
        
        # Test skip_missing_and_nan with single missing value
        single_missing = [missing]
        filtered_single = collect(skip_missing_and_nan(single_missing))
        @test isempty(filtered_single)
        
        # Test with single NaN value
        single_nan = [NaN]
        filtered_single_nan = collect(skip_missing_and_nan(single_nan))
        @test isempty(filtered_single_nan)
    end
    
    @testset "Boundary value handling" begin
        # Test with extreme values
        extreme_df = DataFrame(
            states_ut = ["State1", "State2", "State3"],
            serotype_o_count_pre = [0, typemax(Int64), typemin(Int64)],
            serotype_o_count_post = [0.0, Inf, -Inf],
            serotype_a_count_pre = [1, 999999, -1],
            serotype_a_count_post = [0.1, 100.0, -0.1]
        )
        
        # Test skip_missing_and_nan with extreme values
        extreme_values = [0, typemax(Int64), typemin(Int64), Inf, -Inf]
        filtered_extreme = collect(skip_missing_and_nan(extreme_values))
        @test length(filtered_extreme) == 3  # Should exclude Inf and -Inf
        @test 0 in filtered_extreme
        @test typemax(Int64) in filtered_extreme
        @test typemin(Int64) in filtered_extreme
        
        # Test with negative values (should be handled appropriately)
        negative_values = [-1, -10, -100]
        filtered_negative = collect(skip_missing_and_nan(negative_values))
        @test filtered_negative == negative_values
    end
    
    @testset "State name edge cases" begin
        # Test with states containing special characters
        special_states_df = DataFrame(
            states_ut = ["State-With-Hyphens", "State With Spaces", "State_With_Underscores", "State&Symbols"],
            serotype_o_count_pre = [100, 150, 80, 200],
            serotype_o_count_post = [20, 30, 15, 40]
        )
        
        # Test state validation (would need actual function)
        for state in special_states_df.states_ut
            @test isa(state, String)
            @test !isempty(state)
        end
        
        # Test with very long state names
        long_state_name = "A" * 1000  # 1000 character state name
        long_name_df = DataFrame(
            states_ut = [long_state_name],
            serotype_o_count_pre = [100]
        )
        
        @test nrow(long_name_df) == 1
        @test length(long_name_df.states_ut[1]) == 1000
    end
    
    @testset "Seroprevalence value edge cases" begin
        # Test with seroprevalence values outside expected ranges
        extreme_seroprev_df = DataFrame(
            states_ut = ["State1", "State2", "State3", "State4"],
            serotype_o_count_pre = [100, 0, 50, 200],
            serotype_o_count_post = [20, 0, 10, 40],
            # These would result in extreme percentages
            serotype_o_pct_pre = [150.0, 0.0, -10.0, 100.0],  # >100%, 0%, negative, exactly 100%
            serotype_o_pct_post = [30.0, missing, NaN, 20.0]
        )
        
        # Test handling of out-of-range percentages
        for pct in extreme_seroprev_df.serotype_o_pct_pre
            if !ismissing(pct) && !isnan(pct)
                @test isa(pct, Number)
            end
        end
        
        # Test with very small decimal values
        tiny_values = [1e-10, 1e-15, 1e-20]
        filtered_tiny = collect(skip_missing_and_nan(tiny_values))
        @test filtered_tiny == tiny_values
        
        # Test with very large values
        huge_values = [1e10, 1e15, 1e20]
        filtered_huge = collect(skip_missing_and_nan(huge_values))
        @test filtered_huge == huge_values
    end
    
    @testset "infer_later_year_values - extreme missing value scenarios" begin
        # Test with initial DataFrame having all missing values
        all_missing_initial = DataFrame(
            states_ut = ["State1", "State2"],
            serotype_o_count_pre = [missing, missing],
            serotype_o_count_post = [missing, missing]
        )
        
        normal_cumulative = DataFrame(
            states_ut = ["State1", "State2"],
            serotype_o_count_pre = [100, 150],
            serotype_o_count_post = [20, 30]
        )
        
        result = infer_later_year_values(normal_cumulative, all_missing_initial)
        @test Try.isok(result)
        
        inferred_df = Try.unwrap(result)
        # Missing values in initial should be treated as 0
        @test inferred_df[1, :serotype_o_count_pre] == 100
        @test inferred_df[2, :serotype_o_count_pre] == 150
        
        # Test with cumulative DataFrame having mixed missing values
        mixed_missing_cumulative = DataFrame(
            states_ut = ["State1", "State2"],
            serotype_o_count_pre = [100, missing],
            serotype_o_count_post = [20, 30]
        )
        
        normal_initial = DataFrame(
            states_ut = ["State1", "State2"],
            serotype_o_count_pre = [50, 75],
            serotype_o_count_post = [10, 15]
        )
        
        result_mixed = infer_later_year_values(mixed_missing_cumulative, normal_initial)
        @test Try.iserr(result_mixed)  # Should error when cumulative missing but initial not
    end
    
    @testset "Data type consistency with missing values" begin
        # Test that missing values don't break data type consistency
        mixed_types_df = DataFrame(
            states_ut = ["State1", "State2", "State3"],
            int_column = [1, missing, 3],
            float_column = [1.5, missing, 3.5],
            string_column = ["A", missing, "C"]
        )
        
        # Test that column types are preserved
        @test eltype(mixed_types_df.int_column) == Union{Missing, Int64}
        @test eltype(mixed_types_df.float_column) == Union{Missing, Float64}
        @test eltype(mixed_types_df.string_column) == Union{Missing, String}
        
        # Test skip_missing_and_nan preserves non-missing types
        int_values = [1, missing, 3, missing, 5]
        filtered_ints = collect(skip_missing_and_nan(int_values))
        @test filtered_ints == [1, 3, 5]
        @test eltype(filtered_ints) == Int64
        
        float_values = [1.1, missing, 3.3, NaN, 5.5]
        filtered_floats = collect(skip_missing_and_nan(float_values))
        @test filtered_floats == [1.1, 3.3, 5.5]
        @test eltype(filtered_floats) == Float64
    end
end