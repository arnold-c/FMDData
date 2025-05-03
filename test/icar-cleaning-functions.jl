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

    @testset "Check serotypes" begin
        check_serotype_data = check_allowed_serotypes(cleaned_states_data)
        @test isnothing(check_serotype_data)

        expected_serotypes = ["all", "all", "o", "o", "a", "a", "asia1", "asia1"]
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

        @test isnothing(FMDData._check_all_required_serotypes(all_matched_serotypes))

        try
            FMDData._check_all_required_serotypes(all_matched_additional_missing_serotypes)
        catch e
            @test isequal(
                e,
                AssertionError("Found 2 allowed serotypes ([\"a\", \"o\"]). Required 4: [\"all\", \"o\", \"a\", \"asia1\"]")
            )
        end

        @test isnothing(FMDData._check_no_disallowed_serotypes(all_matched_serotypes))

        try
            FMDData._check_no_disallowed_serotypes(all_matched_additional_missing_serotypes)
        catch e
            @test isequal(
                e,
                AssertionError("Found 1 disallowed serotypes ([\"test\"]).")
            )
        end

    end

    @testset "Check serotype pre and post columns exist" begin
        @test isnothing(check_aggregated_pre_post_counts_exist(cleaned_states_data))
        @test isnothing(check_pre_post_exists(cleaned_states_data))

        missing_pre_post_df = DataFrame(
            "serotype_a_pct_pre" => Float64[],
            "serotype_a_count_post" => Float64[],
            "serotype_o_pct_pre" => Float64[],
            "serotype_o_pct_post" => Float64[],
            "serotype_asia1_pct_pre" => Float64[],
            "serotype_asia1_pct_post" => Float64[],
        )

        try
            check_pre_post_exists(missing_pre_post_df)
        catch e
            @test isequal(
                e,
                ErrorException("All serotype results should have both 'Pre' and 'Post' results columns, only. Instead, the following serotype results have the associated data columns:\nOrderedCollections.OrderedDict{AbstractString, Vector{AbstractString}}(\"serotype_a_pct\" => AbstractString[\"pre\"], \"serotype_a_count\" => AbstractString[\"post\"])")
            )
        end

    end

    @testset "Totals row checks" begin
        @test has_totals_row(cleaned_states_data)

        missing_totals_df = subset(cleaned_states_data, :states_ut => ByRow(s -> !(lowercase(s) in ["totals", "total"])))

        @test !has_totals_row(missing_totals_df)

        incorrect_totals_row_df = DataFrame(
            "states_ut" => ["a", "b", "c", "total"],
            "serotype_all_count_pre" => [10, 10, 10, 30],
            "serotype_all_count_post" => [10, 10, 10, 30],
            "serotype_a_count_pre" => [10, 10, 10, 32],
            "serotype_a_count_post" => [10, 10, 10, 29],
            "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.1],
            "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.0],
        )

        correct_totals_row_df = DataFrame(
            "states_ut" => ["a", "b", "c", "total"],
            "serotype_all_count_pre" => [10, 10, 10, 30],
            "serotype_all_count_post" => [10, 10, 10, 30],
            "serotype_a_count_pre" => [10, 10, 10, 30],
            "serotype_a_count_post" => [10, 10, 10, 30],
            "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
            "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
        )

        try
            all_totals_check(missing_totals_df)
        catch e
            @test isequal(e, AssertionError("Expected 1 row of totals. Found 0. Check the spelling in the states column :states_ut matches the provided `totals_key` \"total\""))
        end

        try
            all_totals_check(incorrect_totals_row_df; atol = 0.1)
        catch e
            @test isequal(e, ErrorException("OrderedCollections.OrderedDict{AbstractString, NamedTuple{(:provided, :calculated)}}(\"serotype_a_count_pre\" => (provided = 32, calculated = 30), \"serotype_a_count_post\" => (provided = 29, calculated = 30), \"serotype_a_pct_pre\" => (provided = 13.1, calculated = 13.3), \"serotype_a_pct_post\" => (provided = 63.0, calculated = 63.3))"))
        end

        @test isnothing(all_totals_check(correct_totals_row_df))
    end

    # @testset "Calculate missing counts/seroprevs" begin
    #     # no_missing_counts_df = DataFrame(
    #     #     "states_ut" => ["a", "b", "c", "total"],
    #     #     "serotype_all_count_pre" => [10, 10, 10, 30],
    #     #     "serotype_all_count_post" => [10, 10, 10, 30],
    #     #     "serotype_a_count_pre" => [10, 10, 10, 30],
    #     #     "serotype_a_count_post" => [10, 10, 10, 30],
    #     #     "serotype_a_pct_pre" => [0.2, 0.1, 0.1, 0.1],
    #     #     "serotype_a_pct_post" => [0.8, 0.6, 0.5, 0.6],
    #     # )
    #     #
    #     # a = calculate_state_counts(no_missing_counts_df)
    #
    #     missing_counts_df = DataFrame(
    #         "states_ut" => ["a", "b", "c", "total"],
    #         "serotype_all_count_pre" => [10, 10, 10, 30],
    #         "serotype_all_count_post" => [10, 10, 10, 30],
    #         "serotype_a_pct_pre" => [0.2, 0.1, 0.1, 0.1],
    #         "serotype_a_pct_post" => [0.8, 0.6, 0.5, 0.6],
    #     )
    #
    #     a = calculate_state_counts(missing_counts_df)
    #
    #     # a = calculate_state_seroprevalence(no_missing_counts_df)
    #     #
    #     # missing_seroprev_df = DataFrame(
    #     #     "states_ut" => ["a", "b", "c", "total"],
    #     #     "serotype_all_count_pre" => [10, 10, 10, 30],
    #     #     "serotype_all_count_post" => [10, 10, 10, 30],
    #     #     "serotype_a_count_pre" => [10, 10, 10, 30],
    #     #     "serotype_a_count_post" => [10, 10, 10, 30],
    #     # )
    #     #
    #     # a = calculate_state_seroprevalence(missing_seroprev_df)
    #
    # end
    #    calculate_state_counts,
    #    calculate_state_seroprevalence
    #    contains_seroprev_results,
    #    contains_count_results,

end
