using FMDData
using DataFrames
using Try: Try

@testset "check-calculated-values.jl" begin

    no_missing_counts_pcts_df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_pre" => [2, 1, 1, 4],
        "serotype_a_count_post" => [8, 6, 5, 19],
        "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
    )

    @test Try.isok(
        check_calculated_values_match_existing(
            DataFrame(
                "serotype_a_count_pre" => [2, 1, 1, 4],
                "serotype_a_count_post" => [8, 6, 5, 19],
                "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
                "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
                "serotype_a_count_post_calculated" => [8, 6, 5, 19],
                "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post_calculated" => [80.0, 60.0, 50.0, 63.3],
            )
        )
    )

    @test Try.isok(
        check_calculated_values_match_existing(
            calculate_state_counts(no_missing_counts_pcts_df)
        )
    )

    @test isequal(
        check_calculated_values_match_existing(
            DataFrame(
                "serotype_a_count_pre" => [2, 1, 1, 4],
                "serotype_a_count_post" => [8, 6, 5, 19],
                "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
                "serotype_a_count_pre_calculated" => [2, 0, 1, 4],
                "serotype_a_count_post_calculated" => [8, 6, 5, 19],
                "serotype_a_pct_pre_calculated" => [20.0, 12.0, 10.0, 13.3],
                "serotype_a_pct_post_calculated" => [80.0, 60.0, 50.0, 63.3],
            )
        ),
        Try.Err("The following calculated columns have discrepancies relative to the provided columns: OrderedCollections.OrderedDict{AbstractString, AbstractString}(\"serotype_a_count_pre\" => \"The following indices (row numbers) differ: [2]. Original: [1]. Calculated: [0]\", \"serotype_a_pct_pre\" => \"The following indices (row numbers) differ: [2]. Original: [10.0]. Calculated: [12.0]\")")
    )
end
