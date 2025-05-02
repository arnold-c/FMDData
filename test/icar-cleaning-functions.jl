using DataFrames

@testset verbose = true "icar-cleaning-functions.jl" begin
    #    clean_colnames,
    #    rename_aggregated_pre_post_counts,
    #    correct_all_state_names,
    #    check_duplicated_columns,
    #    check_duplicated_states,
    #    check_allowed_serotypes,
    #    check_pre_post_exists,
    #    has_totals_row,
    #    all_totals_check,
    #    calculate_state_counts,
    #    calculate_state_seroprevalence
    #
    # collect_all_present_serotypes,
    #    check_aggregated_pre_post_counts_exist,
    #    contains_seroprev_results,
    #    contains_count_results,
    #    correct_state_name

    @testset "Load CSV" begin
        filename = "missing-data.csv"
        not_a_dir = "./not-a-dir.txt"

        @test try
            load_csv("missing-data.tsv", not_a_dir)
        catch e
            isequal(e, ErrorException("$not_a_dir is not a valid directory"))
        end

        dir = "./"

        @test try
            load_csv(filename, dir)
        catch e
            isequal(e, ErrorException("$filename is not within the directory $dir"))
        end

        filename = "test-data.csv"

        data = load_csv(
            filename,
            dir,
            DataFrame
        )

        @test isequal(
            typeof(data),
            DataFrame
        )

        expected_col_names = [
            "States/UT",
            "Pre (N)",
            "Post (N)",
            "Serotype O (%) Pre",
            "Serotype O (%) Post",
            "Serotype A (%) Pre",
            "Serotype A (%) Post",
            "Serotype Asia1 (%) Pre",
            "Serotype Asia1 (%) Post",
        ]

        expected_col_types = Dict(
            zip(
                expected_col_names, [
                    AbstractString,
                    Union{Missing, Integer},
                    Union{Missing, Integer},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                ]
            )
        )

        @test isequal(
            names(data),
            expected_col_names
        )

        for (j, col) in pairs(eachcol(data))
            @test eltype(col) <: expected_col_types[String(j)]
        end

    end

    @testset "Column name cleaning" begin
        dir = "./"

        filename = "test-data.csv"

        data = load_csv(
            filename,
            dir,
            DataFrame
        )

        cleaned_data = clean_colnames(data)

        @test isequal(
            names(cleaned_data),
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

        try
            clean_colnames(special_char_df)
        catch e
            @test isequal(
                e,
                AssertionError("[\"flag-this_column^\"] are columns with disallowed characters.\nDict{String, Vector{RegexMatch}}(\"flag-this_column^\" => [RegexMatch(\"-\"), RegexMatch(\"^\")])")
            )
        end

    end
end
