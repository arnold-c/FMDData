using DataFrames: DataFrame, select, AsTable, Cols

export calculate_state_counts

"""
    calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

Calculate state-specific serotype counts from seroprevalence percentages and total counts.

This function calculates the number of positive samples for each serotype by multiplying 
the seroprevalence percentage by the total number of samples tested for that state and 
vaccination timing (pre/post).

Formula: count = (seroprevalence_pct / 100) * total_count

# Arguments
- `df`: DataFrame containing seroprevalence data
- `allowed_serotypes`: Vector of serotype names to process

# Returns
DataFrame with additional calculated count columns suffixed with "_calculated"

# Note
The calculated values can be compared with existing count columns using 
`check_calculated_values_match_existing()`.
"""
function calculate_state_counts(
        df::DataFrame,
        allowed_serotypes = default_allowed_serotypes
    )
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_pct_(pre|post)\$")
    return hcat(
        df,
        select(
            df,
            AsTable(Cols(reg)) .=> (t -> _calculate_state_counts(t, df)) => AsTable;
            renamecols = true
        )
    )
end

"""
    _calculate_state_counts(table, original_df)

An internal function to handle the calculation of the state/serotype counts based upon the provided state/serotype seroprevalence values and total state counts.
Because DataFrames handles tables as named tuples, we can extract information about the columns being passed from the regex selection and then use substitution strings to collect a view of the correct column of total state counts.

You probably want to use the user-facing function [`calculate_state_counts()`](@ref) instead.
"""
function _calculate_state_counts(table, original_df)
    str_keys = String.(keys(table))
    timing = replace.(str_keys, r"serotype_.*_pct_(pre|post)$" => s"serotype_all_count_\1")
    vals = map(zip(table, timing)) do (seroprev, agg_counts_col)
        original_view = original_df[!, agg_counts_col]
        vals = round.((seroprev / 100) .* original_view)
        return convert.(eltype(original_view), vals)
    end

    names = Symbol.(replace.(str_keys, r"(.*_)pct(_.*)" => s"\1count\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end
