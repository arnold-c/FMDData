using FMDData
using DataFrames
using Try: Try

@testset "calculate-state-seroprevalence.jl" begin
    no_missing_counts_pcts_df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_pre" => [2, 1, 1, 4],
        "serotype_a_count_post" => [8, 6, 5, 19],
        "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
    )

    calculated_df = calculate_state_seroprevalence(no_missing_counts_pcts_df)

    @test isequal(
        names(calculated_df),
        vcat(
            names(no_missing_counts_pcts_df),
            "serotype_a_pct_pre_calculated",
            "serotype_a_pct_post_calculated"
        )
    )

    @test isequal(
        select(
            calculated_df,
            Cols(r".*_calculated")
        ),
        DataFrame(
            "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
            "serotype_a_pct_post_calculated" => [80.0, 60.0, 50.0, 63.3],
        )
    )

    try
        calculate_state_seroprevalence(calculated_df)
    catch e
        @test isequal(
            e,
            ArgumentError("Duplicate variable names: :serotype_a_pct_pre_calculated and :serotype_a_pct_post_calculated. Pass makeunique=true to make them unique using a suffix automatically.")
        )
    end
end
