using Test
using DataFrames
using FMDData
using Try

@testset verbose = true "DataFrame Operations" begin
    @testset "combine_round_dfs" begin
        df1 = DataFrame(state = ["State1"], value = [1], round = ["NADCP 1"])
        df2 = DataFrame(state = ["State2"], value = [2], round = ["NADCP 2"])
        df3 = DataFrame(state = ["State3"], value = [3], round = ["NADCP 3"])

        result = combine_round_dfs(df1, df2, df3)
        @test Try.isok(result)

        combined = Try.unwrap(result)
        @test nrow(combined) == 3
        @test combined.state == ["State1", "State2", "State3"]
        @test combined.value == [1, 2, 3]
        @test combined.round == ["NADCP 1", "NADCP 2", "NADCP 3"]

        # Test with single DataFrame
        single_result = combine_round_dfs(df1)
        @test Try.isok(single_result)
        @test Try.unwrap(single_result) == df1

        # Test with empty DataFrame
        empty_df = DataFrame()
        empty_result = combine_round_dfs(empty_df)
        @test Try.isok(empty_result)
        @test nrow(Try.unwrap(empty_result)) == 0
    end
end