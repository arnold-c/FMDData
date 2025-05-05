using DataFrames
using Try

@testset verbose = true "icar-cleaning-functions.jl" begin

    @testset "Load CSV" begin
        filename = "missing-data.csv"
        not_a_dir = "./not-a-dir.txt"


        @test isequal(
            load_csv("missing-data.tsv", not_a_dir),
            Try.Err("$not_a_dir is not a valid directory")
        )

        dir = "./"


        @test isequal(
            load_csv(filename, dir),
            Try.Err("$filename is not within the directory $dir")
        )

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

        @test isequal(
            clean_colnames(special_char_df),
            Try.Err("[\"flag-this_column^\"] are columns with disallowed characters.\nDict{String, Vector{RegexMatch}}(\"flag-this_column^\" => [RegexMatch(\"-\"), RegexMatch(\"^\")])")
        )
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

        @test isequal(
            FMDData.correct_state_name(
                "New State",
                FMDData.states_dict
            ),
            Try.Err("State name `New State` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling")
        )
    end

    @testset "Check duplicate columns" begin
        @test Try.isok(check_duplicated_column_names(cleaned_states_data))

        similar_column_names_df = DataFrame(
            "states_ut" => String[],
            "states_ut" => String[],
            "seroprevalance_all_count_pre" => Int64[],
            "seroprevalance_all_count_post" => Int64[],
            "seroprevalance_all_pct_pre" => Float64[],
            "seroprevalance_all_count_pre" => Int64[],
            makeunique = true
        )

        @test isequal(
            FMDData._check_similar_column_names(similar_column_names_df),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_1\"]).")
        )

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

        @test isequal(
            FMDData._check_similar_column_names(similar_column_names_df_2),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_u\" => [\"states_ut\", \"states_ut_1\", \"states_ut_1_2\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_test\"]).")
        )


        @test isequal(
            check_duplicated_column_names(similar_column_names_df),
            Try.Err("Similar column names were found in the data: OrderedCollections.OrderedDict(\"states_ut\" => [\"states_ut_1\"], \"seroprevalance_all_count_pre\" => [\"seroprevalance_all_count_pre_1\"]).")
        )

        duplicate_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 1:10,
            :d => 3:12,
            :e => 2:11,
        )


        @test isequal(
            check_duplicated_columns(duplicate_column_vals_df),
            Try.Err("Found columns with identical values: [[:a, :c], [:b, :e]]")
        )

        unique_column_vals_df = DataFrame(
            :a => 1:10,
            :b => 2:11,
            :c => 3:12,
        )

        @test Try.isok(check_duplicated_columns(unique_column_vals_df))

    end

    @testset "Check missing states" begin
        @test Try.isok(check_missing_states(cleaned_states_data))

        missing_states_df = DataFrame("states_ut" => ["a", "b", missing, "a", missing])

        @test isequal(
            check_missing_states(missing_states_df),
            Try.Err("There are 2 values in the states_ut column that are of type `Missing`")
        )

    end

    @testset "Check duplicated states" begin
        @test Try.isok(check_duplicated_states(cleaned_states_data))

        duplicated_states_df = DataFrame("states_ut" => ["a", "b", "c", "a"])


        @test isequal(
            check_duplicated_states(duplicated_states_df),
            Try.Err("The dataframe has 4 state values, but only 3 unique state values. (\"a\",) were duplicated")
        )
    end

    @testset "Check serotypes" begin
        @test Try.isok(check_allowed_serotypes(cleaned_states_data))

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

    @testset "Check seroprevalence cols are %" begin
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

    @testset "Check serotype pre and post columns exist" begin
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

    @testset "Totals row checks" begin
        @test Try.isok(has_totals_row(cleaned_states_data))

        missing_totals_df = subset(cleaned_states_data, :states_ut => ByRow(s -> !(lowercase(s) in ["totals", "total"])))

        @test isequal(
            has_totals_row(missing_totals_df),
            Try.Err("Totals row not found in the data using the possible row keys [\"total\", \"totals\"] in the column :states_ut")
        )

        incorrect_totals_row_df = DataFrame(
            "states_ut" => ["a", "b", "c", "total"],
            "serotype_all_count_pre" => [10, 10, 10, 30],
            "serotype_all_count_post" => [10, 10, 10, 30],
            "serotype_a_count_pre" => [2, 1, 1, 5],
            "serotype_a_count_post" => [8, 6, 5, 20],
            "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.1],
            "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.0],
        )

        correct_totals_row_df = DataFrame(
            "states_ut" => ["a", "b", "c", "total"],
            "serotype_all_count_pre" => [10, 10, 10, 30],
            "serotype_all_count_post" => [10, 10, 10, 30],
            "serotype_a_count_pre" => [2, 1, 1, 4],
            "serotype_a_count_post" => [8, 6, 5, 19],
            "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
            "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
        )


        @test isequal(
            all_totals_check(missing_totals_df),
            Try.Err("Expected 1 row of totals. Found 0. Check the spelling in the states column :states_ut matches the provided `totals_key` \"total\"")
        )


        @test isequal(
            all_totals_check(incorrect_totals_row_df; atol = 0.1),
            Try.Err(
                "There were discrepancies in the totals calculated and those provided in the data: OrderedCollections.OrderedDict{AbstractString, NamedTuple{(:provided, :calculated)}}(\"serotype_a_count_pre\" => (provided = 5, calculated = 4), \"serotype_a_count_post\" => (provided = 20, calculated = 19), \"serotype_a_pct_pre\" => (provided = 13.1, calculated = 13.3), \"serotype_a_pct_post\" => (provided = 63.0, calculated = 63.3))"
            )
        )


        @test Try.isok(all_totals_check(correct_totals_row_df))
    end

    @testset "Calculate missing counts/seroprevs" begin
        no_missing_counts_pcts_df = DataFrame(
            "states_ut" => ["a", "b", "c", "total"],
            "serotype_all_count_pre" => [10, 10, 10, 30],
            "serotype_all_count_post" => [10, 10, 10, 30],
            "serotype_a_count_pre" => [2, 1, 1, 4],
            "serotype_a_count_post" => [8, 6, 5, 19],
            "serotype_a_pct_pre" => [20.0, 10.0, 10.0, 13.3],
            "serotype_a_pct_post" => [80.0, 60.0, 50.0, 63.3],
        )

        @test isequal(
            DataFrame(
                "serotype_a_count_pre_calculated" => [2, 1, 1, 4],
                "serotype_a_count_post_calculated" => [8, 6, 5, 19],
            ),
            select(
                calculate_state_counts(no_missing_counts_pcts_df),
                Cols(r".*_calculated")
            )
        )

        @test isequal(
            DataFrame(
                "serotype_a_pct_pre_calculated" => [20.0, 10.0, 10.0, 13.3],
                "serotype_a_pct_post_calculated" => [80.0, 60.0, 50.0, 63.3],
            ),
            select(
                calculate_state_seroprevalence(no_missing_counts_pcts_df),
                Cols(r".*_calculated")
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

end
