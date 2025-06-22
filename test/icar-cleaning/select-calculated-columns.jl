using FMDData
using DataFrames
using Try: Try

@testset verbose = true "select-calculated-columns.jl" begin

    df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_pre" => [2, 1, 1, 4],
        "serotype_a_count_post" => [8, 6, 5, 19],
        "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
        "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
        "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
    )

    out = select_calculated_cols!(df)

    @test isequal(
        out,
        Try.Ok(nothing)
    )

    @test isequal(
        names(df),
        [
            "states_ut",
            "serotype_all_count_pre",
            "serotype_all_count_post",
            "serotype_a_count_pre",
            "serotype_a_count_post",
            "serotype_a_pct_post",
            "serotype_a_pct_pre",
        ]
    )

    @test isequal(
        select_calculated_cols!(
            DataFrame(
                "states_ut" => ["a", "b", "c", "total"],
                "serotype_all_count_pre" => [10, 10, 10, 30],
                "serotype_all_count_post" => [10, 10, 10, 30],
                "serotype_a_count_pre" => [2, 1, 1, 4],
                "serotype_a_count_post" => [8, 6, 5, 19],
                "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
                "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
                "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
            );
            reg = Regex(
                "serotype_(?:$(join(["all", "a"], "|")))_(count|pct)_(testing)_(?:pre|post)\$"
            )
        ),
        Try.Err("No columns were matched by the regex. Check it correctly identifies the appropriate serotype data columns")
    )

    @test isequal(
        select_calculated_cols!(
            DataFrame(
                "states_ut" => ["a", "b", "c", "total"],
                "serotype_all_count_pre" => [10, 10, 10, 30],
                "serotype_all_count_post" => [10, 10, 10, 30],
                "serotype_a_count_testing_pre" => [2, 1, 1, 4],
                "serotype_a_count_post" => [8, 6, 5, 19],
                "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
                "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
                "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
            );
            reg = Regex(
                "serotype_(?:$(join(["all", "a"], "|")))_(count|pct)_(testing)_(?:pre|post)\$"
            )
        ),
        Try.Err(
            "Only 1 capture group should exist for the column serotype_a_count_testing_pre. Found 2: Union{Nothing, SubString{String}}[\"count\", \"testing\"]."
        )
    )

    @test isequal(
        select_calculated_cols!(
            DataFrame(
                "states_ut" => ["a", "b", "c", "total"],
                "serotype_all_count_pre" => [10, 10, 10, 30],
                "serotype_all_count_post" => [10, 10, 10, 30],
                "serotype_a_testing_pre" => [2, 1, 1, 4],
                "serotype_a_count_post" => [8, 6, 5, 19],
                "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
                "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
                "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
            );
            reg = Regex(
                "serotype_(?:$(join(["all", "a"], "|")))_(testing)_(?:pre|post)\$"
            )
        ),
        Try.Err(
            "The capture group is not expected. It should be one of [\"count\", \"pct\"], but instead it is testing"
        )
    )

    missing_count_df = DataFrame(
        "states_ut" => ["a", "b", "c", "total"],
        "serotype_all_count_pre" => [10, 10, 10, 30],
        "serotype_all_count_post" => [10, 10, 10, 30],
        "serotype_a_count_post" => [8, 6, 5, 19],
        "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
        "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
        "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
        "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
    )

    select_calculated_cols!(missing_count_df)

    @test isequal(
        names(missing_count_df),
        [
            "states_ut",
            "serotype_all_count_pre",
            "serotype_all_count_post",
            "serotype_a_count_post",
            "serotype_a_pct_post",
            "serotype_a_count_pre",
            "serotype_a_pct_pre",
        ]
    )
end
