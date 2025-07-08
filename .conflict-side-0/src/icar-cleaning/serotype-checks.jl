using DataFrames: DataFrame
using Try: Try


export check_allowed_serotypes

"""
    check_allowed_serotypes(
        df::DataFrame,
        allowed_serotypes::Vector{String} = vcat("all", default_allowed_serotypes),
        reg::Regex = r"serotype_(.*)_(?|count|pct)_(pre|post)"
    )

Function to confirm that all required and no disallowed serotypes are provided in the data.
"""
function check_allowed_serotypes(
        df::DataFrame,
        allowed_serotypes::Vector{String} = vcat("all", default_allowed_serotypes),
        reg::Regex = r"serotype_(.*)_(?|count|pct)_(pre|post)"
    )
    all_matched_serotypes = unique(collect_all_present_serotypes(df, reg))
    required_check = _check_all_required_serotypes(all_matched_serotypes, allowed_serotypes)
    disallowed_check = _check_no_disallowed_serotypes(all_matched_serotypes, allowed_serotypes)
    if !Try.iserr(required_check) && !Try.iserr(disallowed_check)
        return Try.Ok(nothing)
    end
    return Try.Err(_combine_error_messages([required_check, disallowed_check]))
end

"""
    collect_all_present_serotypes(df::DataFrame, reg::Regex)

Return a vector of all column names that contain serotype information specified in the regex.
"""
function collect_all_present_serotypes(
        df::DataFrame,
        reg::Regex = r"serotype_(.*)_(?|count|pct)_(pre|post)"
    )
    colnames = names(df)
    all_matched_cols = filter(!isnothing, match.(reg, colnames))
    all_matched_serotypes = map(m -> String(m[1]), all_matched_cols)

    return all_matched_serotypes
end

"""
    _check_all_required_serotypes(
        all_matched_serotypes::T,
        allowed_serotypes::T = default_allowed_serotypes,
    ) where {T <: AbstractVector{<:AbstractString}}

Internal function to check that all required serotypes provided in the data.
"""
function _check_all_required_serotypes(
        all_matched_serotypes::T,
        allowed_serotypes::T = vcat("all", default_allowed_serotypes)
    ) where {T <: AbstractVector{<:AbstractString}}
    matched_serotypes = unique(filter(m -> in(m, allowed_serotypes), all_matched_serotypes))
    length(matched_serotypes) == length(allowed_serotypes) ||
        return Try.Err("Found $(length(matched_serotypes)) allowed serotypes ($matched_serotypes). Required $(length(allowed_serotypes)): $allowed_serotypes.")
    return Try.Ok(nothing)
end

"""
    _check_no_disallowed_serotypes(
        all_matched_serotypes::T,
        allowed_serotypes::T = default_allowed_serotypes,
    ) where {T <: AbstractVector{<:AbstractString}}

Internal function to check that there are no disallowed serotypes provided in the data.
"""
function _check_no_disallowed_serotypes(
        all_matched_serotypes::T,
        allowed_serotypes::T = vcat("all", default_allowed_serotypes)
    ) where {T <: AbstractVector{<:AbstractString}}
    matched_serotypes = unique(filter(m -> !in(m, allowed_serotypes), all_matched_serotypes))
    length(matched_serotypes) == 0 ||
        return Try.Err("Found $(length(matched_serotypes)) disallowed serotypes ($matched_serotypes).")
    return Try.Ok(nothing)
end
