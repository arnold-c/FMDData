using FMDData
using DataFrames
using Try: Try
using OrderedCollections: OrderedCollections

@testset verbose = true "total-row-functions.jl" begin
    cleaned_states_data = DataFrame(
        "states_ut" => String["a", "b", "total"],
        "serotype_all_count_pre" => Int64[10, 10, 20],
        "serotype_all_count_post" => Int64[10, 10, 20],
        "serotype_a_pct_pre" => Float64[50.0, 25.0, 37.5],
        "serotype_a_count_pre" => Int64[10, 10, 20],
        "serotype_a_pct_post" => Float64[50.0, 25.0, 37.5],
        "serotype_a_count_post" => Int64[10, 10, 20],
        "serotype_o_pct_pre" => Float64[50.0, 25.0, 37.5],
        "serotype_o_pct_post" => Float64[50.0, 25.0, 37.5],
        "serotype_asia1_pct_pre" => Float64[50.0, 25.0, 37.5],
        "serotype_asia1_pct_post" => Float64[50.0, 25.0, 37.5],
    )

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

        @test occursin(
            r"There were discrepancies in the totals calculated and those provided in the data: (OrderedCollections\.)?OrderedDict\{AbstractString, NamedTuple\{\(:provided, :calculated\)\}\}\(\"serotype_a_count_pre\" => \(provided = 5, calculated = 4\), \"serotype_a_count_post\" => \(provided = 20, calculated = 19\), \"serotype_a_pct_pre\" => \(provided = 13.1, calculated = 13.3\), \"serotype_a_pct_post\" => \(provided = 63.0, calculated = 63.3\)\)",
            Try.unwrap_err(all_totals_check(incorrect_totals_row_df; atol=0.1))
        )

        @test Try.isok(all_totals_check(correct_totals_row_df))

        incorrect_totals_calculated = OrderedCollections.OrderedDict(
            "serotype_all_count_pre" => 30,
            "serotype_all_count_post" => 30,
            "serotype_a_count_pre" => 4,
            "serotype_a_count_post" => 19,
            "serotype_a_pct_pre" => 13.3,
            "serotype_a_pct_post" => 63.3,
        )

        @test occursin(
            r"There were discrepancies in the totals calculated and those provided in the data: (OrderedCollections\.)?OrderedDict\{AbstractString, NamedTuple\{\(:provided, :calculated\)\}\}\(\"serotype_a_count_pre\" => \(provided = 5, calculated = 4\).*\"serotype_a_count_post\" => \(provided = 20, calculated = 19\).*\"serotype_a_pct_pre\" => \(provided = 13\.1, calculated = 13\.3\).*\"serotype_a_pct_post\" => \(provided = 63\.0, calculated = 63\.3\)",
            Try.unwrap_err(
                totals_check(
                    incorrect_totals_row_df[end, Not(:states_ut)],
                    incorrect_totals_calculated
                )
            ),
        )

        # @test isequal(
        #     totals_check(
        #         incorrect_totals_row_df[end, Not(:states_ut)],
        #         incorrect_totals_calculated
        #     ),
        #     Try.Err("There were discrepancies in the totals calculated and those provided in the data: OrderedDict{AbstractString, NamedTuple{(:provided, :calculated)}}(\"serotype_a_count_pre\" => (provided = 5, calculated = 4), \"serotype_a_count_post\" => (provided = 20, calculated = 19), \"serotype_a_pct_pre\" => (provided = 13.1, calculated = 13.3), \"serotype_a_pct_post\" => (provided = 63.0, calculated = 63.3))")
        # )
    end

    @testset verbose = true "Calculate totals" begin
        @test Try.isok(calculate_all_totals(cleaned_states_data))

        @test isequal(
            Try.unwrap(calculate_all_totals(cleaned_states_data)),
            OrderedCollections.OrderedDict(
                "serotype_all_count_pre" => 20,
                "serotype_all_count_post" => 20,
                "serotype_a_pct_pre" => 37.5,
                "serotype_a_count_pre" => 20,
                "serotype_a_pct_post" => 37.5,
                "serotype_a_count_post" => 20,
                "serotype_o_pct_pre" => 37.5,
                "serotype_o_pct_post" => 37.5,
                "serotype_asia1_pct_pre" => 37.5,
                "serotype_asia1_pct_post" => 37.5,
            )
        )

        @test isequal(
            calculate_all_totals(cleaned_states_data[begin:(end-1), :]),
            Try.Err("Expected 1 row of totals. Found 0. Check the spelling in the states column :states_ut matches the provided `totals_key` \"total\"")
        )

        count_total_dict = OrderedCollections.OrderedDict()
        FMDData._calculate_totals!(
            count_total_dict,
            cleaned_states_data[begin:(end-1), "serotype_a_count_pre"],
            "serotype_a_count_pre"
        )
        @test isequal(
            count_total_dict,
            OrderedCollections.OrderedDict("serotype_a_count_pre" => 20)
        )


        seroprev_total_dict = OrderedCollections.OrderedDict()
        FMDData._calculate_totals!(
            seroprev_total_dict,
            cleaned_states_data[begin:(end-1), "serotype_a_pct_pre"],
            "serotype_a_pct_pre",
            cleaned_states_data[begin:(end-1), "serotype_all_count_pre"],
            20,
            1
        )
        @test isequal(
            seroprev_total_dict,
            OrderedCollections.OrderedDict("serotype_a_pct_pre" => 37.5)
        )

    end

    @testset verbose = true "Argument selection to calculate totals" begin
        @test Try.isok(FMDData._totals_row_selectors(cleaned_states_data))

        @test isequal(
            Try.unwrap(FMDData._totals_row_selectors(cleaned_states_data)),
            (3, cleaned_states_data[:, 2:end])
        )

        @test isequal(
            calculate_all_totals(cleaned_states_data[begin:(end-1), :]),
            Try.Err("Expected 1 row of totals. Found 0. Check the spelling in the states column :states_ut matches the provided `totals_key` \"total\"")
        )

        # Test when targeting a count column
        @test Try.isok(
            FMDData._collect_totals_check_args(
                cleaned_states_data[1:2, 5],
                names(cleaned_states_data)[5],
                cleaned_states_data,
                3,
                FMDData.default_allowed_serotypes,
                2
            )
        )

        @test isequal(
            Try.unwrap(
                FMDData._collect_totals_check_args(
                    cleaned_states_data[1:2, 5],
                    names(cleaned_states_data)[5],
                    cleaned_states_data,
                    3,
                    FMDData.default_allowed_serotypes,
                    2
                )
            ),
            ([10, 10], "serotype_a_count_pre")
        )

        @test Try.isok(
            FMDData._collect_totals_check_args(
                cleaned_states_data[1:2, 6],
                names(cleaned_states_data)[6],
                cleaned_states_data,
                3,
                FMDData.default_allowed_serotypes,
                2
            )
        )

        # Test when targeting a % column
        @test isequal(
            Try.unwrap(
                FMDData._collect_totals_check_args(
                    cleaned_states_data[1:2, 6],
                    names(cleaned_states_data)[6],
                    cleaned_states_data,
                    3,
                    FMDData.default_allowed_serotypes,
                    2
                )
            ),
            ([50.0, 25.0], "serotype_a_pct_post", [10, 10], 20, 2)
        )


    end

    @testset "Select calculated totals" begin

        both_totals_df = DataFrame(
            "states_ut" => ["a", "Total", "Total calculated"],
            "vals" => [1, 2, 3]
        )

        @test isequal(
            select_calculated_totals!(both_totals_df),
            Try.Ok(nothing)
        )

        @test isequal(
            both_totals_df,
            DataFrame(
                "states_ut" => ["a", "Total"],
                "vals" => [1, 3]
            )
        )

        both_totals_unordered_df = DataFrame(
            "states_ut" => ["a", "Total calculated", "Total"],
            "vals" => [1, 3, 2]
        )

        @test isequal(
            select_calculated_totals!(both_totals_unordered_df),
            Try.Ok(nothing)
        )

        @test isequal(
            both_totals_unordered_df,
            DataFrame(
                "states_ut" => ["a", "Total"],
                "vals" => [1, 3]
            )
        )

        @test isequal(
            select_calculated_totals!(
                DataFrame(
                    "states_ut" => ["total", "total"],
                    "vals" => [1, 2]
                )
            ),
            Try.Err("Expected to only find one row titled \"total\", but instead found 2")
        )

        @test isequal(
            select_calculated_totals!(
                DataFrame(
                    "states_ut" => ["total calculated", "total calculated"],
                    "vals" => [1, 2]
                )
            ),
            Try.Err("Expected to only find one row titled \"total calculated\", but instead found 2")
        )

        @test isequal(
            select_calculated_totals!(
                DataFrame(
                    "states_ut" => ["a", "Total"],
                    "vals" => [1, 2]
                )
            ),
            Try.Ok("Only has provided totals. Continuing")
        )

        @test isequal(
            select_calculated_totals!(
                DataFrame(
                    "states_ut" => ["a", "Total calculated"],
                    "vals" => [1, 3]
                )
            ),
            Try.Err("Data contains the calculated totals row, but not the provided one")
        )

        @test isequal(
            select_calculated_totals!(
                DataFrame(
                    "states_ut" => ["a", "b"],
                    "alterative_column" => ["total", "total calculated"],
                    "vals" => [1, 2]
                )
            ),
            Try.Err("Data contains neither calculated or provided totals rows with a key in the column :states_ut")
        )
    end

end
