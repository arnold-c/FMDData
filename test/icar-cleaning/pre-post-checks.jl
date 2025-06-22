using FMDData
using DataFrames
using Try: Try
using OrderedCollections: OrderedCollections

@testset verbose = true "pre-post-checks.jl" begin
    cleaned_states_data = DataFrame(
        "serotype_all_count_pre" => Int64[],
        "serotype_all_count_post" => Int64[],
        "serotype_a_pct_pre" => Float64[],
        "serotype_a_count_pre" => Int64[],
        "serotype_a_pct_post" => Float64[],
        "serotype_a_count_post" => Int64[],
        "serotype_o_pct_pre" => Float64[],
        "serotype_o_pct_post" => Float64[],
        "serotype_asia1_pct_pre" => Float64[],
        "serotype_asia1_pct_post" => Float64[],
    )

    @test Try.isok(check_aggregated_pre_post_counts_exist(cleaned_states_data))
    @test Try.isok(check_pre_post_exists(cleaned_states_data))

    missing_pre_post_df = DataFrame(
        "serotype_a_pct_pre" => Float64[],
        "serotype_a_count_post" => Float64[],
        "serotype_o_pct_pre" => Float64[],
        "serotype_o_pct_post" => Float64[],
        "serotype_asia1_pct_pre" => Float64[],
        "serotype_asia1_pct_post" => Float64[],
    )

    @test isequal(
        check_aggregated_pre_post_counts_exist(missing_pre_post_df),
        Try.Err("The aggregated count columns [\"serotype_all_count_pre\", \"serotype_all_count_post\"] do not exist in the data")
    )


    @test isequal(
        check_pre_post_exists(missing_pre_post_df),
        Try.Err("All serotype results should have both 'Pre' and 'Post' results columns, only. Instead, the following serotype results have the associated data columns:\nOrderedCollections.OrderedDict{AbstractString, Vector{AbstractString}}(\"serotype_a_pct\" => AbstractString[\"pre\"], \"serotype_a_count\" => AbstractString[\"post\"])")
    )
end
