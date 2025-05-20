using DataFrames: DataFrame, select, AsTable, Cols

export calculate_state_counts

"""
    calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

A wrapper function around the internal `_calculate_state_counts()` function to calculate the state/serotype specific counts based upon the state/serotype seroprevalence values and total state counts. See the documentation of `_calculate_state_counts()` for more details on the implementation.
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
