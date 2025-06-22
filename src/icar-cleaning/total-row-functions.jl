using DataFrames: DataFrame, DataFrameRow, ncol, Not, Cols, select
using Try: Try
using OrderedCollections: OrderedDict

export has_totals_row,
    all_totals_check,
    calculate_all_totals,
    totals_check,
    select_calculated_totals!

"""
    has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )

Check if the table has a totals row.

`df` should have, at the very least, cleaned column names using the [`clean_colnames()`](@ref) function.
"""
function has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )
    length(filter(s -> in(s, possible_keys), lowercase.(df[!, column]))) > 0 ||
        return Try.Err("Totals row not found in the data using the possible row keys $possible_keys in the column :$column")
    return Try.Ok(nothing)
end


"""
    all_totals_check(
        df::DataFrame;
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex,
        atol = 0.0,
        digits = 1
    )

Check if all provided values in the provided totals row are correct. If the column is a count, then calculate an unweighted sum. If the column is the seroprevalence, calculated the sum weighted by the relevant counts (pre- or post-vaccination counts).
"""
function all_totals_check(
        df::DataFrame;
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$"),
        atol = 0.0,
        digits = 1
    )

    totals_dict = Try.@? calculate_all_totals(
        df;
        column = column,
        totals_key = totals_key,
        allowed_serotypes = allowed_serotypes,
        reg = reg,
        digits = digits
    )

    return all_totals_check(
        totals_dict,
        df;
        column = column,
        totals_key = totals_key,
        allowed_serotypes = allowed_serotypes,
        reg = reg,
        atol = atol
    )
end

function all_totals_check(
        totals_dict::OrderedDict,
        df::DataFrame;
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$"),
        atol = 0.0,
    )

    totals_rn, selected_df = Try.@? _totals_row_selectors(
        df,
        column,
        totals_key;
        reg = reg
    )

    length(totals_dict) == ncol(selected_df) || return Try.Err("The number of totals calculated is $(length(totals_dict)), but there are $(ncol(selected_df)) columns selected to have totals calculated for")

    Try.@? totals_check(
        selected_df[totals_rn, :],
        totals_dict,
        column;
        atol = atol,
    )

    return Try.Ok(nothing)

end

"""
    calculate_all_totals(
        df::DataFrame;
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex,
        digits = 1
    )

Calculate all totals using the appropriate method instance of the internal function [`_calculate_totals!()`](@ref), dependent on whether the column is a Float (seroprevalence) or Integer (count). Uses the internal function [`_collect_totals_check_args()`](@ref) to identify what arguments need to be passed to [`_calculate_totals!()`](@ref) function. Uses the internal function [`_totals_row_selectors()`](@ref) to extract the totals row from the dataframe, for use when calculating the serotype weight total seroprevalence.
"""
function calculate_all_totals(
        df::DataFrame;
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$"),
        digits = 1
    )
    totals_rn, selected_df = Try.@? _totals_row_selectors(
        df,
        column,
        totals_key;
        reg = reg
    )

    totals_dict = OrderedDict{AbstractString, Real}()

    for col_ind in eachindex(names(selected_df))
        totals_check_args = _collect_totals_check_args(
            selected_df[Not(totals_rn), col_ind],
            names(selected_df)[col_ind],
            selected_df,
            totals_rn,
            allowed_serotypes,
            digits
        )
        Try.iserr(totals_check_args) && return totals_check_args
        _calculate_totals!(totals_dict, Try.unwrap(totals_check_args)...)
    end

    return Try.Ok(totals_dict)
end

"""
    _totals_row_selectors(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total";
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex

    )

Internal function to extract the totals row and the subset of dataframe rows that match the regex.
"""
function _totals_row_selectors(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total";
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$")

    )
    totals_rn = findall(lowercase.(df[!, column]) .== totals_key)
    length(totals_rn) == 1 || return Try.Err("Expected 1 row of totals. Found $(length(totals_rn)). Check the spelling in the states column :$column matches the provided `totals_key` \"$totals_key\"")
    totals_rn = totals_rn[1]
    selected_df = select(df, Cols(reg))
    return Try.Ok((totals_rn, selected_df))
end

"""
    _collect_totals_check_args(
        col::Vector{T},
        totals::DataFrame,
        colname::String,
        _...
    ) where {T <: Union{Union{<:Missing, <:Integer}, <:Integer}}
        col::Vector{T},
        totals::DataFrame,
        colname::String,
        _...
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}

Collect the necessary arguments to provide to the [`totals_check()`](@ref) functions. When checking the totals on counts, use `_...` varargs to denote additional arguments can be passed (necessary for total checks on seroprevalence values) but will not be assigned an used within the function body.

Returns a tuple of variables to be unpacked and passed to [`totals_check()`](@ref)
"""
function _collect_totals_check_args(
        col::Vector{T},
        colname::String,
        _...
    ) where {T <: Union{Union{<:Missing, <:Integer}, <:Integer}}
    return Try.Ok((col, colname))
end

function _collect_totals_check_args(
        col::Vector{T},
        colname::String,
        df::DataFrame,
        totals_rn,
        allowed_serotypes = default_allowed_serotypes,
        digits = 1,
    ) where {T <: Union{Union{<:Missing, <:AbstractFloat}, <:AbstractFloat}}
    # Forms the regex string: r"serotype_(?|o|a|asia1)_pct_(pre|post)$"
    # (?|...) indicates a non-capture group i.e. must match any of the words separated by '|' characters, but does not return a match as a capture group
    # (pre|post) is the only capture group, providing the timing used to collect the correct state column for weighting the seroprevalence sums
    reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_pct_(pre|post)\$")
    denom_type_matches = match(reg, colname)
    length(denom_type_matches) == 1 || return Try.Err("For column $colname, $(length(denom_type_matches)) possible denominators found, but only expected 1: $(denom_type_matches.captures)")
    denom_type = denom_type_matches[1]
    denom_colname = "serotype_all_count_$denom_type"

    # Calculate own aggregate pre/post total in case provided values are incorrect
    denom_col = df[Not(totals_rn), denom_colname]
    denom_total = sum(skipmissing(denom_col))

    return Try.Ok((col, colname, denom_col, denom_total, digits))
end

"""
    _calculate_totals!(
        totals_dict::OrderedDict,
        col::Vector{T},
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}

Internal function to calculate the serotype total.
"""
function _calculate_totals!(
        totals_dict::OrderedDict,
        col::Vector{T},
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}
    calculated_total = sum(skip_missing_and_nan(col))
    totals_dict[colname] = calculated_total
    return nothing
end

function _calculate_totals!(
        totals_dict::OrderedDict,
        col::Vector{T},
        colname::String,
        denom_col::Vector{C},
        denom_total,
        digits = 1
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
    calculated_total = round(
        sum(skip_missing_and_nan(col .* denom_col)) / denom_total;
        digits = digits
    )
    totals_dict[colname] = calculated_total
    return nothing
end

"""
    totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}

Check if the provided total counts equal the sum calculated using the provided state counts.
"""
function totals_check(
        totals::DataFrameRow,
        calculated_totals::OrderedDict,
        column::Symbol = :states_ut;
        atol = 0.0
    )
    errors_dict = OrderedDict{AbstractString, NamedTuple{(:provided, :calculated)}}()

    for colname in names(totals)
        provided_total = totals[colname]
        calculated_total = calculated_totals[colname]
        if !isapprox(provided_total, calculated_total; atol = atol)
            errors_dict[colname] = (provided_total, calculated_total)
        end
    end

    if !isempty(errors_dict)
        return Try.Err("There were discrepancies in the totals calculated and those provided in the data: $errors_dict")
    end

    return Try.Ok(nothing)
end

"""
    select_calculated_totals!(
    	df::DataFrame,
    	column::Symbol = :states_ut,
    	totals_key = "total",
    	calculated_totals_key = "total calculated"
    )

If the cleaned data contains both a provided and a calculated totals row then return strip the provided one and rename the calculated.
"""
function select_calculated_totals!(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total",
        calculated_totals_key = "total calculated"
    )
    provided_totals_rn = findall(lowercase.(df[!, column]) .== totals_key)
    length(provided_totals_rn) <= 1 || return Try.Err("Expected to only find one row titled \"$totals_key\", but instead found $(length(provided_totals_rn))")

    calculated_totals_rn = findall(lowercase.(df[!, column]) .== calculated_totals_key)
    length(calculated_totals_rn) <= 1 || return Try.Err("Expected to only find one row titled \"$calculated_totals_key\", but instead found $(length(calculated_totals_rn))")

    has_provided_totals = false
    if length(provided_totals_rn) == 1
        provided_totals_rn = provided_totals_rn[1]
        has_provided_totals = true
    end
    has_calculated_totals = false
    if length(calculated_totals_rn) == 1
        calculated_totals_rn = calculated_totals_rn[1]
        has_calculated_totals = true
    end
    if has_provided_totals && !has_calculated_totals
        return Try.Ok("Only has provided totals. Continuing")
    end
    if has_calculated_totals && !has_provided_totals
        return Try.Err("Data contains the calculated totals row, but not the provided one")
    end
    if !has_provided_totals && !has_calculated_totals
        return Try.Err("Data contains neither calculated or provided totals rows with a key in the column :$column")
    end

    show_warnings && @warn "Using calculated totals"
    df[calculated_totals_rn, column] = titlecase("Total")
    popat!(df, provided_totals_rn)

    return Try.Ok(nothing)
end
