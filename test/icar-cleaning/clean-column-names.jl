using FMDData
using DataFrames
using Try: Try

@testset "clean-column-names.jl" begin
    data = DataFrame(
        "States/UT" => [],
        "Pre (N)" => [],
        "Post (N)" => [],
        "Serotype O (%) Pre" => [],
        "Serotype O (%) Post" => [],
        "Serotype A (%) Pre" => [],
        "Serotype A (%) Post" => [],
        "Serotype Asia1 (%) Pre" => [],
        "Serotype Asia1 (%) Post" => [],
    )

    cleaned_colname_data = Try.@? clean_colnames(data)

    @test isequal(
        names(cleaned_colname_data),
        [
            "states_ut",
            "pre_count",
            "post_count",
            "serotype_o_pct_pre",
            "serotype_o_pct_post",
            "serotype_a_pct_pre",
            "serotype_a_pct_post",
            "serotype_asia1_pct_pre",
            "serotype_asia1_pct_post",
        ]
    )

    special_char_df = DataFrame(
        "States/UT" => String[],
        "flag-this_column^" => Int64[]
    )

    @test isequal(
        clean_colnames(special_char_df),
        Try.Err("[\"flag-this_column^\"] are columns with disallowed characters.\nDict{String, Vector{RegexMatch}}(\"flag-this_column^\" => [RegexMatch(\"-\"), RegexMatch(\"^\")])")
    )
end
