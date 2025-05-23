using DataFrames: DataFrame, select, Cols
using OrderedCollections: OrderedDict
using Try: Try
using StatsBase: mean

export check_seroprevalence_as_pct

"""
    check_seroprevalence_as_pct(df::DataFrame, reg::Regex)

Check if all seroprevalence columns are reported as a percentage, and not as a proportion.
"""
function check_seroprevalence_as_pct(
        df::DataFrame,
        reg::Regex = Regex("serotype_(?|$(join(default_allowed_serotypes, "|")))_pct_(pre|post)\$")
    )
    prop_cols_dict = OrderedDict{Symbol, AbstractFloat}()
    for (name, vals) in pairs(eachcol(select(df, Cols(reg))))
        if round(mean(skip_missing_and_nan(vals)); digits = 2) < 1.0
            prop_cols_dict[name] = round(mean(skip_missing_and_nan(vals)); digits = 2)
        end
    end
    if !isempty(prop_cols_dict)
        return Try.Err("All `pct` columns should be a %, not a proportion. The following columns are likely reported as proportions with associated mean values: $prop_cols_dict")
    end
    return Try.Ok(nothing)
end
