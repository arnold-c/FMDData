using Test
using DataFrames
using OrderedCollections
using FMDData
using Try

@testset verbose = true "Metadata Operations" begin
    @testset "add_metadata_col! - single DataFrame" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        # Test adding integer metadata
        @test Try.isok(add_metadata_col!(:year, df => 2021))
        @test df.year == [2021, 2021]

        # Test adding string metadata
        @test Try.isok(add_metadata_col!(:test_type, df => "SPCE"))
        @test df.test_type == ["SPCE", "SPCE"]

        # Test adding float metadata
        @test Try.isok(add_metadata_col!(:threshold, df => 1.65))
        @test df.threshold == [1.65, 1.65]
    end

    @testset "add_metadata_col! - multiple DataFrames" begin
        df1 = DataFrame(state = ["State1"], value = [1])
        df2 = DataFrame(state = ["State2"], value = [2])

        @test Try.isok(add_metadata_col!(:year, df1 => 2021, df2 => 2022))
        @test df1.year == [2021]
        @test df2.year == [2022]
    end

    @testset "add_all_metadata!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        # Test valid metadata
        metadata = OrderedDict(
            :sample_year => "Combined",
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )

        @test Try.isok(add_all_metadata!(df => metadata))
        @test df.sample_year == ["Combined", "Combined"]
        @test df.report_year == [2022, 2022]
        @test df.round_name == ["NADCP 2", "NADCP 2"]
        @test df.test_type == ["SPCE", "SPCE"]
        @test df.test_threshold == ["1.65 log10 @ 35% inhibition", "1.65 log10 @ 35% inhibition"]

        # Test invalid metadata key
        invalid_metadata = OrderedDict(:invalid_key => "value")
        df_invalid = DataFrame(state = ["State1"], value = [1])
        result = add_all_metadata!(df_invalid => invalid_metadata)
        @test Try.iserr(result)
        @test contains(Try.unwrap_err(result), "Metadata provided that is not accepted")
    end

    @testset "add_test_threshold!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        @test Try.isok(add_test_threshold!(df => "1.65 log10"))
        @test df.test_threshold == ["1.65 log10", "1.65 log10"]

        # Test custom column name
        df2 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_test_threshold!(df2 => "custom threshold"; threshold_column = :custom_thresh))
        @test df2.custom_thresh == ["custom threshold"]
    end

    @testset "add_test_type!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        @test Try.isok(add_test_type!(df => "SPCE"))
        @test df.test_type == ["SPCE", "SPCE"]

        # Test custom column name
        df2 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_test_type!(df2 => "ELISA"; test_column = :test_method))
        @test df2.test_method == ["ELISA"]
    end

    @testset "add_round_name!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        @test Try.isok(add_round_name!(df => "NADCP 1"))
        @test df.round == ["NADCP 1", "NADCP 1"]

        # Test custom column name
        df2 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_round_name!(df2 => "Round 2"; round_column = :round_name))
        @test df2.round_name == ["Round 2"]
    end

    @testset "add_report_year!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        @test Try.isok(add_report_year!(df => 2021))
        @test df.report_year == [2021, 2021]

        # Test custom column name
        df2 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_report_year!(df2 => 2022; year_column = :year))
        @test df2.year == [2022]
    end

    @testset "add_sample_year!" begin
        df = DataFrame(state = ["State1", "State2"], value = [1, 2])

        @test Try.isok(add_sample_year!(df => 2021))
        @test df.sample_year == [2021, 2021]

        # Test with string
        df2 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_sample_year!(df2 => "Combined"))
        @test df2.sample_year == ["Combined"]

        # Test custom column name
        df3 = DataFrame(state = ["State1"], value = [1])
        @test Try.isok(add_sample_year!(df3 => 2020; year_column = :year))
        @test df3.year == [2020]
    end

    @testset "Edge Cases - DataFrame handling" begin
        @testset "Empty DataFrame handling" begin
            # Test with completely empty DataFrame
            empty_df = DataFrame()

            # Test functions that should handle empty DataFrames gracefully
            @test nrow(empty_df) == 0
            @test ncol(empty_df) == 0

            # Test with DataFrame with columns but no rows
            empty_with_cols = DataFrame(
                states_ut = String[],
                serotype_o_count_pre = Int[],
                serotype_o_count_post = Int[]
            )

            @test nrow(empty_with_cols) == 0
            @test ncol(empty_with_cols) == 3

            # Test metadata addition to empty DataFrame
            metadata_result = add_metadata_col!(:test_year, empty_with_cols => 2021)
            @test Try.isok(metadata_result)
            @test hasproperty(empty_with_cols, :test_year)  # Use hasproperty instead of names comparison
            @test isempty(empty_with_cols.test_year)
        end

        @testset "Single-row DataFrame handling" begin
            # Test with single-row DataFrame
            single_row_df = DataFrame(
                states_ut = ["Test State"],
                serotype_o_count_pre = [100],
                serotype_o_count_post = [20]
            )

            # Test metadata addition
            metadata_result = add_metadata_col!(:sample_year, single_row_df => 2021)
            @test Try.isok(metadata_result)
            @test single_row_df.sample_year == [2021]

            # Test with single row containing missing values
            single_row_missing = DataFrame(
                states_ut = ["Test State"],
                serotype_o_count_pre = [missing],
                serotype_o_count_post = [20]
            )

            # Test metadata addition still works
            metadata_result2 = add_metadata_col!(:test_type, single_row_missing => "SPCE")
            @test Try.isok(metadata_result2)
            @test single_row_missing.test_type == ["SPCE"]
        end

        @testset "Data type consistency with missing values" begin
            # Test that missing values don't break data type consistency
            mixed_types_df = DataFrame(
                states_ut = ["State1", "State2", "State3"],
                int_column = [1, missing, 3],
                float_column = [1.5, missing, 3.5],
                string_column = ["A", missing, "C"]
            )

            # Test that column types are preserved
            @test eltype(mixed_types_df.int_column) == Union{Missing, Int64}
            @test eltype(mixed_types_df.float_column) == Union{Missing, Float64}
            @test eltype(mixed_types_df.string_column) == Union{Missing, String}

            # Test metadata addition preserves existing types
            metadata_result = add_metadata_col!(:year, mixed_types_df => 2021)
            @test Try.isok(metadata_result)
            @test eltype(mixed_types_df.int_column) == Union{Missing, Int64}
            @test eltype(mixed_types_df.float_column) == Union{Missing, Float64}
            @test eltype(mixed_types_df.string_column) == Union{Missing, String}
        end
    end
end

