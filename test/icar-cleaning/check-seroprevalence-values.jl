using FMDData
using DataFrames
using OrderedCollections: OrderedDict
using Try: Try

@testset "check-seroprevalence-values.jl" begin
    seroprevs_as_pct_df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_pre" => [10, 10, 10, 30],
        "serotype_a_count_post" => [10, 10, 10, 30],
        "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
    )

    seroprevs_as_props_df = transform(
        seroprevs_as_pct_df,
        Cols(r".*_pct_.*") .=> ByRow(c -> round(c / 100; digits = 1)),
        renamecols = false
    )

    seroprevs_with_missing_df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_pre" => [10, 10, 10, 30],
        "serotype_a_count_post" => [10, 10, 10, 30],
        "serotype_a_pct_pre" => [20.0, missing, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
    )

    @test Try.isok(check_seroprevalence_as_pct(seroprevs_as_pct_df))

    @test isequal(
        check_seroprevalence_as_pct(seroprevs_as_props_df),
        Try.Err("All `pct` columns should be a %, not a proportion. The following columns are likely reported as proportions with associated mean values: OrderedCollections.OrderedDict{Symbol, AbstractFloat}(:serotype_a_pct_pre => 0.12, :serotype_a_pct_post => 0.62)")
    )

    @test Try.isok(check_seroprevalence_as_pct(seroprevs_with_missing_df))
end
