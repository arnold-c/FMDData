using DataFrames: DataFrame
using OrderedCollections: OrderedDict
using Try: Try

export check_calculated_values_match_existing

"""
    check_calculated_values_match_existing(
        df::DataFrame,
        allowed_serotypes::T = default_allowed_serotypes;
        digits = 1
    ) where {T <: AbstractVector{<:AbstractString}}

Check whether the provided values of counts and seroprevalence values match the corresponding values calculated.
"""
function check_calculated_values_match_existing(
        df::DataFrame,
        allowed_serotypes::T = default_allowed_serotypes;
        digits = 1
    ) where {T <: AbstractVector{<:AbstractString}}
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$")

    colnames = String.(names(df))
    cols = filter(!isnothing, match.(reg, colnames))
    miscalculation_dict = OrderedDict{AbstractString, AbstractString}()
    for col in cols
        original = col.match
        calculated_col = original * "_calculated"
        if in(calculated_col, colnames)
            if df[!, original] != df[!, calculated_col]
                original_vals = df[!, original]
                calculated_vals = df[!, calculated_col]
                diffidxs = findall(original_vals .!= calculated_vals)
                miscalculation_dict[original] = "The following indices (row numbers) differ: $diffidxs. Original: $(original_vals[diffidxs]). Calculated: $(calculated_vals[diffidxs])"
            end
        end
    end
    if !isempty(miscalculation_dict)
        return Try.Err("The following calculated columns have discrepancies relative to the provided columns: $miscalculation_dict")
    end

    return Try.Ok(nothing)
end
