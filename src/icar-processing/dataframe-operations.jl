using DataFrames: DataFrame
using Try

export combine_round_dfs


"""
    combine_round_dfs(dfs::DataFrame...)

Combines multiple DataFrames into a single DataFrame.
"""
function combine_round_dfs(
        dfs::DataFrame...
    )
    return Try.Ok(vcat(dfs...))
end