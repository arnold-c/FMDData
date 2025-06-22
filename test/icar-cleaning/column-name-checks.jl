using FMDData
using DataFrames
using OrderedCollections: OrderedDict
using Try: Try

@testset "column-name-checks.jl" begin
    @testset "Check duplicate columns" begin
        different_column_names_df = DataFrame(
            "states_ut" => String[],
            "serotype_all_count_pre" => Int64[],
            "serotype_all_count_post" => Int64[],
            "serotype_all_pct_pre" => Float64[],
            "serotype_a_count_pre" => Int64[],
            makeunique = true
        )

        similar_column_names_df = DataFrame(
            "states_ut" => String[],
            "states_ut" => String[],
            "serotype_all_count_pre" => Int64[],
            "serotype_all_count_post" => Int64[],
            "serotype_all_pct_pre" => Float64[],
            "serotype_all_count_pre" => Int64[],
            makeunique = true
        )

        @test Try.isok(check_duplicated_column_names(different_column_names_df))

        @test Try.isok(FMDData._check_identical_column_names(different_column_names_df))

        @test isequal(
            FMDData._check_similar_column_names(similar_column_names_df),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"serotype_all_count_pre\" => [\"serotype_all_count_pre_1\"]).")
        )

        similar_column_names_df_2 = DataFrame(
            "states_ut_1_2" => String[],
            "states_ut" => String[],
            "states_ut_1" => String[],
            "states_u" => String[],
            "serotype_all_count_pre" => Int64[],
            "serotype_all_count_post" => Int64[],
            "serotype_all_pct_pre" => Float64[],
            "serotype_all_count_pre_test" => Int64[],
        )

        @test isequal(
            FMDData._check_similar_column_names(similar_column_names_df_2),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_u\" => [\"states_ut\", \"states_ut_1\", \"states_ut_1_2\"], \"serotype_all_count_pre\" => [\"serotype_all_count_pre_test\"]).")
        )


        @test isequal(
            check_duplicated_column_names(similar_column_names_df),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"serotype_all_count_pre\" => [\"serotype_all_count_pre_1\"]).")
        )

        duplicate_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 1:10,
            :d => 3:12,
            :e => 2:11,
        )


        @test isequal(
            check_duplicated_columns(duplicate_column_vals_df),
            Try.Err("Found columns with identical values: [[:a, :c], [:b, :e]]")
        )

        unique_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 3:12,
        )

        @test Try.isok(check_duplicated_columns(unique_column_vals_df))

    end
end
