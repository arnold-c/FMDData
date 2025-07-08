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
end