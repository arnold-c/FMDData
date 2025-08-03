using DataFrames: DataFrame, select, Cols, AsTable
export calculate_state_seroprevalence

"""
    calculate_state_seroprevalence(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

Calculate state-specific serotype seroprevalence percentages from count data.

This function calculates the seroprevalence percentage for each serotype by dividing 
the serotype-specific positive count by the total number of samples tested for that 
state and vaccination timing (pre/post).

Formula: seroprevalence_pct = (serotype_count / total_count) * 100

# Arguments
- `df`: DataFrame containing count data
- `allowed_serotypes`: Vector of serotype names to process
- `reg`: Regex pattern to identify count columns (optional)
- `digits`: Number of decimal places for percentages (default: 1)

# Returns
DataFrame with additional calculated seroprevalence columns suffixed with "_calculated"

# Note
The calculated values can be compared with existing percentage columns using 
`check_calculated_values_match_existing()`.
"""
function calculate_state_seroprevalence(
        df::DataFrame,
        allowed_serotypes::T = default_allowed_serotypes;
        reg = Regex("serotype_(?:$(join(allowed_serotypes, "|")))_count_(pre|post)\$"),
        digits = 1
    ) where {T <: AbstractVector{<:AbstractString}}
    return hcat(
        df,
        select(
            df,
            AsTable(Cols(reg)) .=> (
                t -> _calculate_state_seroprevalence(t, df; reg = reg, digits = digits)
            ) => AsTable;
            renamecols = true
        )
    )
end

"""
    _calculate_state_seroprevalence(table, original_df)

An internal function to handle the calculation of the state/serotype counts based upon the provided state/serotype seroprevalence values and total state counts.
Because DataFrames handles tables as named tuples, we can extract information about the columns being passed from the regex selection and then use substitution strings to collect a view of the correct column of total state counts.

You probably want to use the user-facing function [`calculate_state_seroprevalence()`](@ref) instead.
"""
function _calculate_state_seroprevalence(
        table,
        original_df;
        reg = Regex("serotype_(?:$(join(default_allowed_serotypes, "|")))_count_(pre|post)\$"),
        digits = 1
    )
    str_keys = String.(keys(table))
    timing = replace.(str_keys, reg => s"serotype_all_count_\1")
    vals = map(
        ((serotype_count, agg_counts_col),) -> round.((serotype_count ./ @view(original_df[!, agg_counts_col])) .* 100; digits = digits),
        zip(table, timing)
    )

    names = Symbol.(replace.(str_keys, r"(.*_)count(_.*)" => s"\1pct\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end
