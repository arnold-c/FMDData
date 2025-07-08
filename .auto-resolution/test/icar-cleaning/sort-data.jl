using FMDData
using DataFrames
using Try: Try

@testset verbose = true "sort-data.jl" begin
    @testset verbose = true "Sort dataframe columns" begin
        unsorted_df = DataFrame(
            "states_ut" => String[],
            "serotype_all_count_pre" => Int64[],
            "serotype_all_count_post" => Int64[],
            "serotype_o_pct_pre" => Float64[],
            "serotype_o_count_pre" => Int64[],
            "serotype_o_count_post" => Int64[],
            "serotype_a_pct_pre" => Float64[],
            "serotype_asia1_pct_pre" => Float64[],
            "serotype_asia1_pct_post" => Float64[],
            "serotype_a_count_pre" => Int64[],
            "serotype_asia1_count_pre" => Int64[],
        )

        @test isequal(
            sort_columns!(unsorted_df),
            Try.Ok(nothing)
        )

        @test isequal(
            names(unsorted_df),
            [
                "states_ut",
                "serotype_all_count_pre",
                "serotype_all_count_post",
                "serotype_o_count_pre",
                "serotype_o_pct_pre",
                "serotype_o_count_post",
                "serotype_a_count_pre",
                "serotype_a_pct_pre",
                "serotype_asia1_count_pre",
                "serotype_asia1_pct_pre",
                "serotype_asia1_pct_post",
            ]
        )

    end

    @testset verbose = true "Sort dataframe rows by state" begin

        unsorted_df = DataFrame(
            "states_ut" => String["a", "d", "Total", "c", "b", "e"],
            "original_idx" => [1, 2, 3, 4, 5, 6]
        )

        @test isequal(
            sort_states!(unsorted_df),
            Try.Ok(nothing)
        )

        @test isequal(
            unsorted_df,
            DataFrame(
                "states_ut" => String["a", "b", "c", "d", "e", "Total"],
                "original_idx" => [1, 5, 4, 2, 6, 3]
            )
        )

        unsorted_duplicates_df = DataFrame(
            "states_ut" => String["a", "d", "Total", "c", "a", "d", "b", "e", "total"],
            "original_idx" => [1, 2, 3, 4, 5, 6, 7, 8, 9]
        )

        @test isequal(
            sort_states!(unsorted_duplicates_df),
            Try.Ok(nothing)
        )

        @test isequal(
            unsorted_duplicates_df,
            DataFrame(
                "states_ut" => String["a", "a", "b", "c", "d", "d", "e", "Total", "total"],
                "original_idx" => [1, 5, 7, 4, 2, 6, 8, 3, 9]
            )
        )


    end
end
