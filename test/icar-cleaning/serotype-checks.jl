using FMDData
using DataFrames
using Try: Try

@testset verbose = true "serotype-checks.jl" begin
    cleaned_states_data = DataFrame(
        "states_ut" => String[],
        "serotype_all_count_pre" => Int64[],
        "serotype_all_count_post" => Int64[],
        "serotype_o_count_pre" => Int64[],
        "serotype_o_pct_pre" => Float64[],
        "serotype_o_count_post" => Int64[],
        "serotype_o_pct_post" => Float64[],
        "serotype_a_count_pre" => Int64[],
        "serotype_a_pct_pre" => Float64[],
        "serotype_a_count_post" => Int64[],
        "serotype_a_pct_post" => Float64[],
        "serotype_asia1_count_pre" => Int64[],
        "serotype_asia1_pct_pre" => Float64[],
        "serotype_asia1_count_post" => Int64[],
        "serotype_asia1_pct_post" => Float64[],
    )

    @test Try.isok(check_allowed_serotypes(cleaned_states_data))

    expected_serotypes = [
        "all",
        "all",
        "o",
        "o",
        "o",
        "o",
        "a",
        "a",
        "a",
        "a",
        "asia1",
        "asia1",
        "asia1",
        "asia1",
    ]
    @test isequal(
        expected_serotypes,
        FMDData.collect_all_present_serotypes(cleaned_states_data)
    )

    additional_missing_serotypes_df = DataFrame(
        "serotype_a_pct_pre" => Float64[],
        "serotype_a_pct_post" => Float64[],
        "serotype_o_pct_pre" => Float64[],
        "serotype_o_pct_post" => Float64[],
        "serotype_test_pct_pre" => Float64[],
    )
    additional_missing_expected_serotypes = ["a", "a", "o", "o", "test"]

    @test isequal(
        additional_missing_expected_serotypes,
        FMDData.collect_all_present_serotypes(additional_missing_serotypes_df)
    )

    all_matched_serotypes = unique(expected_serotypes)
    all_matched_additional_missing_serotypes = unique(additional_missing_expected_serotypes)

    @test Try.isok(FMDData._check_all_required_serotypes(all_matched_serotypes))


    @test isequal(
        FMDData._check_all_required_serotypes(all_matched_additional_missing_serotypes),
        Try.Err("Found 2 allowed serotypes ([\"a\", \"o\"]). Required 4: [\"all\", \"o\", \"a\", \"asia1\"].")
    )

    @test Try.isok(FMDData._check_no_disallowed_serotypes(all_matched_serotypes))


    @test isequal(
        FMDData._check_no_disallowed_serotypes(all_matched_additional_missing_serotypes),
        Try.Err("Found 1 disallowed serotypes ([\"test\"]).")
    )

    @test isequal(
        check_allowed_serotypes(additional_missing_serotypes_df),
        Try.Err("Found 2 allowed serotypes ([\"a\", \"o\"]). Required 4: [\"all\", \"o\", \"a\", \"asia1\"]. Found 1 disallowed serotypes ([\"test\"]).")
    )
end
