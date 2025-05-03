using DataFrames

@testset verbose = true "icar-cleaning-functions.jl" begin

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

    dir = "./"
    filename = "test-data.csv"
    data = load_csv(
        filename,
        dir,
        DataFrame
    )
    cleaned_colname_data = clean_colnames(data)

    @testset "Column name cleaning" begin
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

        try
            clean_colnames(special_char_df)
        catch e
            @test isequal(
                e,
                AssertionError("[\"flag-this_column^\"] are columns with disallowed characters.\nDict{String, Vector{RegexMatch}}(\"flag-this_column^\" => [RegexMatch(\"-\"), RegexMatch(\"^\")])")
            )
        end
    end

    renamed_aggregated_counts_df = rename_aggregated_pre_post_counts(cleaned_colname_data)
    @testset "Rename aggregated Pre/Post counts" begin
        @test isequal(
            names(renamed_aggregated_counts_df),
            [
                "states_ut",
                "serotype_all_count_pre",
                "serotype_all_count_post",
                "serotype_o_pct_pre",
                "serotype_o_pct_post",
                "serotype_a_pct_pre",
                "serotype_a_pct_post",
                "serotype_asia1_pct_pre",
                "serotype_asia1_pct_post",
            ]
        )
    end

    cleaned_states_data = correct_all_state_names(
        renamed_aggregated_counts_df,
        :states_ut,
        FMDData.states_dict
    )

    @testset "Correcting state names" begin
        for n in cleaned_states_data[!, :states_ut]
            @test in(n, [values(FMDData.states_dict)..., "Total"])
        end

        try
            FMDData.correct_state_name(
                "New State",
                FMDData.states_dict
            )
        catch e
            @test isequal(
                e,
                ErrorException("State name `New State` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling")
            )
        end
    end

    @testset "Check duplicate columns" begin
        check_duplicate_column_names_data = check_duplicated_column_names(cleaned_states_data)
        @test isnothing(check_duplicate_column_names_data)

        similar_column_names_df = DataFrame(
            "states_ut" => String[],
            "states_ut" => String[],
            "seroprevalance_all_count_pre" => Int64[],
            "seroprevalance_all_count_post" => Int64[],
            "seroprevalance_all_pct_pre" => Float64[],
            "seroprevalance_all_count_pre" => Int64[],
            makeunique = true
        )

        try
            FMDData._check_similar_column_names(similar_column_names_df)
        catch e
            @test isequal(
                e,
                ErrorException("Similar column names were found in the data:\nOrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_1\"])")
            )
        end

        similar_column_names_df_2 = DataFrame(
            "states_ut_1_2" => String[],
            "states_ut" => String[],
            "states_ut_1" => String[],
            "states_u" => String[],
            "seroprevalance_all_count_pre" => Int64[],
            "seroprevalance_all_count_post" => Int64[],
            "seroprevalance_all_pct_pre" => Float64[],
            "seroprevalance_all_count_pre_test" => Int64[],
        )

        try
            FMDData._check_similar_column_names(similar_column_names_df_2)
        catch e
            @test isequal(
                e,
                ErrorException("Similar column names were found in the data:\nOrderedCollections.OrderedDict(\"states_u\" => [\"states_ut\", \"states_ut_1\", \"states_ut_1_2\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_test\"])")
            )
        end


        try
            check_duplicated_column_names(similar_column_names_df)
        catch e
            @test isequal(
                e,
                ErrorException("Similar column names were found in the data:\nOrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_1\"])")
            )
        end

        duplicate_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 1:10,
            :d => 3:12,
            :e => 2:11,
        )

        try
            check_duplicated_columns(duplicate_column_vals_df)
        catch e
            @test isequal(
                e,
                ErrorException("Found columns with identical values: [[:a, :c], [:b, :e]]")
            )
        end

        unique_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 3:12,
        )

        @test isnothing(check_duplicated_columns(unique_column_vals_df))

    end

    @testset "Check missing states" begin
        check_missing_states_data = check_missing_states(cleaned_states_data)
        @test isnothing(check_missing_states_data)

        missing_states_df = DataFrame("states_ut" => ["a", "b", missing, "a", missing])

        try
            check_missing_states(missing_states_df)
        catch e
            @test isequal(
                e,
                AssertionError("There are 2 values in the states_ut column that are of type `Missing`")
            )
        end

    end

    @testset "Check duplicated states" begin
        check_duplicated_states_data = check_duplicated_states(cleaned_states_data)
        @test isnothing(check_duplicated_states_data)

        duplicated_states_df = DataFrame("states_ut" => ["a", "b", "c", "a"])

        try
            check_duplicated_states(duplicated_states_df)
        catch e
            @test isequal(
                e,
                AssertionError("The dataframe has 4 state values, but only 3 unique state values. (\"a\",) were duplicated")
            )
        end
    end
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

end
