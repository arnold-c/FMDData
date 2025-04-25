using CSV: read
using DataFrames: DataFrame, select, subset, filter, rename, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol

export load_csv,
    clean_colnames,
    all_totals_check,
    has_totals_row,
    check_duplicated_states,
    check_duplicated_columns,
    check_allowed_serotypes,
    check_aggregated_pre_post_counts,
    rename_aggregated_pre_post_counts,
    check_pre_post_exists,
    correct_state_name,
    correct_all_state_names,
    calculate_state_counts,
    calculate_state_seroprevalence

default_allowed_serotypes::Vector{String} = ["o", "a", "asia1"]

"""
    load_csv(
        filename::T1,
        dir::T1,
        output_format = DataFrame
    ) where {T1 <: AbstractString}

A helper function to check if a csv input file and directory exists, and if so, load (as a DataFrame by default).
"""
function load_csv(
        filename::T1,
        dir::T1,
        output_format = DataFrame
    ) where {T1 <: AbstractString}
    isdir(dir) || error("$dir is not a valid directory")
    contains(filename, r".*\.csv$")    || error("$filename is not a csv file")

    dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(dir))
    in(filename, dir_files) || error("$filename is not within the directory $dir")

    return read(
        joinpath(dir, filename),
        output_format
    )
end

"""
    clean_colnames(df::DataFrame)

Replace spaces and / with underscores
"""
function clean_colnames(df::DataFrame)
    return rename(
        t -> lowercase(replace(t, "/" => "_", " " => "_")),
        df
    )
end

"""
    rename_aggregated_pre_post_counts(
        df::DataFrame,
        original_regex::Regex
        substitution_string::SubstitutionString
    )

Rename the aggregated pre/post counts to use the same format as the serotype-specific values
"""
function rename_aggregated_pre_post_counts(
        df::DataFrame,
        original_regex::Regex = r"^(pre|post)_\(n\)",
        substitution_string::SubstitutionString = s"serotype_all_(n)_\1"
    )
    return rename(
        s -> replace(s, original_regex => substitution_string),
        df
    )
end

"""
    check_duplicated_columns(df::DataFrame)

Check if the provided data has any duplicate column names
"""
function check_duplicated_columns(df::DataFrame)
    df_ncol = ncol(df)
    colnames = names(df)
    unique_colnames = unique(colnames)

    colname_counts = NamedTuple{tuple(Symbol.(unique_colnames)...)}(
        map(
            i -> sum(i .== colnames),
            unique_colnames,
        )
    )

    @assert df_ncol == length(unique_colnames) "The dataframe has $df_ncol columns, but only $(length(unique_colnames)) uniques column names. $(keys(filter(c -> values(c) != 1, colname_counts))) were duplicated"
    return nothing
end

"""
    check_duplicated_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )

Check if there are duplicated states in the data
"""
function check_duplicated_states(
        df::DataFrame,
        column::Symbol = :states_ut,
        allowed_serotypes = default_allowed_serotypes
    )
    return @assert length(df[!, column]) == length(unique(df[!, column]))
end

"""
    check_allowed_serotypes(
        df::DataFrame,
        allowed_serotypes::Vector{String} = default_allowed_serotypes,
        reg::Regex
    )

Function to confirm that all required and no disallowed serotypes are provided in the data.
"""
function check_allowed_serotypes(
        df::DataFrame,
        allowed_serotypes::Vector{String} = vcat("all", default_allowed_serotypes),
        reg::Regex = r"serotype_(.*)_\(.\)_(pre|post)"
    )
    all_matched_serotypes = unique(collect_all_present_serotypes(df))
    _check_all_required_serotypes(all_matched_serotypes, allowed_serotypes)
    _check_no_disallowed_serotypes(all_matched_serotypes, allowed_serotypes)
    return nothing
end

"""
    collect_all_present_serotypes(df::DataFrame, reg::Regex)

Return a vector of all column names that contain serotype information specified in the regex.
"""
function collect_all_present_serotypes(
        df::DataFrame,
        reg::Regex = r"serotype_(.*)_\(.\)_(pre|post)"
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
    @assert length(matched_serotypes) == length(allowed_serotypes) "Found $(length(matched_serotypes)) allowed serotypes ($matched_serotypes). Required $(length(allowed_serotypes)): $allowed_serotypes"
    return nothing
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
    @assert length(matched_serotypes) == 0 "Found $(length(matched_serotypes)) disallowed serotypes ($matched_serotypes)."
    return nothing

end

"""
    check_pre_post_exists(df::DataFrame, reg::Regex)

Confirms each combination of serotype and result type (N/%) has both a pre- and post-vaccination results column, but nothing else.
"""
function check_pre_post_exists(df::DataFrame, reg::Regex = r"serotype_(.*)_\((.)\)_(pre|post)")
    colnames = names(df)

    all_matched_columns = filter(
        !isnothing,
        match.(r"(serotype_.*_\(.\))_(pre|post)", colnames)
    )

    unique_serotype_result = unique(map(m -> m[1], all_matched_columns))

    for serotype in unique_serotype_result
        pre_post_matches = filter(m -> m[1] == serotype, all_matched_columns)
        @assert length(unique(pre_post_matches)) == 2 "Serotype results $(serotype) should have both a 'Pre' and 'Post' results column. Instead, data contains $(length(unique(pre_post_matches)))columns: $(map(m -> m[2], pre_post_matches))"
    end

    return nothing
end

"""
    check_aggregated_pre_post_counts(
        df::DataFrame,
    )

Check if data contains aggregated counts of pre and post vaccinated individuals
"""
function check_aggregated_pre_post_counts(df::DataFrame, columns = ["pre_(n)", "post_(n)"])
    return @assert sum(map(c -> in(c, names(df)), columns)) == length(columns)
end


"""
    has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )

Check if the table has a totals row.

`df` should have, at the very least, cleaned column names using the `clean_colnames()` function.
"""
function has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )
    return length(filter(s -> in(s, possible_keys), lowercase.(df[!, column]))) > 0
end

function contains_seroprev_results(df, serotypes = ["all", "o", "a", "asia1"])

end

function contains_count_results(df, serotypes = ["all", "o", "a", "asia1"])

end

"""
    all_totals_check(df::DataFrame, totals_key = "total")

Check if all provided values in the provided totals row are correct. If the column is a count, then calculate an unweighted sum. If the column is the seroprevalence, calculated the sum weighted by the relevant counts (pre- or post-vaccination counts).
"""
function all_totals_check(df::DataFrame, totals_key = "total")
    totals = subset(df, :states_ut => s -> lowercase.(s) .== totals_key)
    @assert nrow(totals) == 1
    totals_rn = indexin((totals_key,), lowercase.(df.states_ut))
    col_names = names(df)
    for col_ind in eachindex(names(df))
        col_name = col_names[col_ind]
        if col_name == "states_ut"
            continue
        end
        totals_check_args = _collect_totals_check_args(
            df[Not(totals_rn), col_ind],
            totals,
            names(df)[col_ind],
            df,
            totals_rn,
        )
        _totals_check(totals_check_args...)
    end
    return nothing
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

Collect the necessary arguments to provide to the `totals_check()` functions. When checking the totals on counts, use `_...` varargs to denote additional arguments can be passed (necessary for total checks on seroprevalence values) but will not be assigned an used within the function body.

Returns a tuple of variables to be unpacked and passed to `totals_check()`
"""
function _collect_totals_check_args(
        col::Vector{T},
        totals::DataFrame,
        colname::String,
        _...
    ) where {T <: Union{Union{<:Missing, <:Integer}, <:Integer}}
    return (col, totals[1, colname], colname)
end

function _collect_totals_check_args(
        col::Vector{T},
        totals::DataFrame,
        colname::String,
        df::DataFrame,
        totals_rn,
        allowed_serotypes = default_allowed_serotypes
    ) where {T <: Union{Union{<:Missing, <:AbstractFloat}, <:AbstractFloat}}
    # Forms the regex string: r"serotype_(?|o|a|asia1)_\(%\)_(pre|post)$"
    # (?|...) indicates a non-capture group i.e. must match any of the words separated by '|' characters, but does not return a match as a capture group
    # (pre|post) is the only capture group, providing the timing used to collect the correct state column for weighting the seroprevalence sums
    reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_\\(%\\)_(pre|post)\$")
    denom_type_matches = match(reg, colname)
    @assert length(denom_type_matches) == 1 "For column $colname, $(length(denom_type_matches)) possible denominators found, but only expected 1: $(denom_type_matches.captures)"
    denom_type = denom_type_matches[1]
    denom_colname = "serotype_all_(n)_$denom_type"

    denom_col = df[Not(totals_rn), denom_colname]
    denom_total = sum(skipmissing(denom_col))

    return (col, totals[1, colname], colname, denom_col, denom_total)
end

"""
    totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}

Check if the provided total counts equal the sum calculated using the provided state counts.
"""
function _totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}
    calculated_total = sum(skipmissing(col))
    if calculated_total != provided_total
        @warn "`$colname`: $calculated_total doesn't match provided total $provided_total"
    end
    return nothing
end

"""
    _totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
        denom_col::Vector{C},
        denom_total
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
        col::Vector{T},
        provided_total,
        colname::String,
        denom_col::Vector{C},
        denom_total
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }

Check if the provided total for serotype seroprevalence values equal a weighted sum based on reported total counts.
"""
function _totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
        denom_col::Vector{C},
        denom_total
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
    calculated_total = sum(skipmissing(col .* denom_col)) / denom_total
    if !isapprox(calculated_total, provided_total; atol = 0.2)
        @warn "`$colname`: $calculated_total doesn't match provided total $provided_total"
    end
    return nothing
end


"""
    correct_all_state_names(
        df::DataFrame,
        column::Symbol = :states_ut,
        states_dict::Dict = FMDData.states_dict
    )

Correct all state name values in the data
"""
function correct_all_state_names(
        df::DataFrame,
        column::Symbol = :states_ut,
        states_dict::Dict = FMDData.states_dict
    )
    df_state_keys = df[!, column]

    return transform(
        df,
        column => ByRow(s -> correct_state_name(s, states_dict));
        renamecols = false
    )

end

"""
	correct_state_name(
        input_name::String,
        states_dict::Dict = FMDData.states_dict
    )

Check if a state name is correctly spelled, or previously characterized and matched with a correct name. Returns the correct name if possible, or errors.
"""
function correct_state_name(
        input_name::String,
        states_dict::Dict = FMDData.states_dict
    )
    possible_state_values = values(states_dict)

    if in(input_name, possible_state_values) || lowercase(input_name) == "total"
        return input_name
    end

    possible_state_keys = keys(states_dict)
    in(input_name, possible_state_keys) ||
        error("State name `$input_name` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling")

    return states_dict[input_name]
end


"""
    calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

A wrapper function around the internal `_calculate_state_counts()` function to calculate the state/serotype specific counts based upon the state/serotype seroprevalence values and total state counts. See the documentation of `_calculate_state_counts()` for more details on the implementation.
"""
function calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_\\(n\\)_(pre|post)\$")
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
    timing = replace.(str_keys, r"serotype_.*_\(%\)_(\w+)$" => s"serotype_all_(n)_\1")
    vals = map(zip(table, timing)) do (seroprev, agg_counts_col)
        original_view = @view(original_df[!, agg_counts_col])
        vals = round.((seroprev / 100) .* original_view)
        return convert.(eltype(original_view), vals)
    end

    names = Symbol.(replace.(str_keys, r"(.*_)\(%\)(_.*)" => s"\1(n)\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end

"""
    calculate_state_seroprevalence(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

A wrapper function around the internal `_calculate_state_seroprevalence()` function to calculate the state/serotype specific counts based upon the state/serotype seroprevalence values and total state counts. See the documentation of `_calculate_state_seroprevalence()` for more details on the implementation.
"""
function calculate_state_seroprevalence(
        df::DataFrame,
        allowed_serotypes::T = default_allowed_serotypes,
    ) where {T <: AbstractVector{<:AbstractString}}
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_\\(n\\)_(pre|post)\$")
    return hcat(
        df,
        select(
            df,
            AsTable(Cols(reg)) .=> (t -> _calculate_state_seroprevalence(t, df)) => AsTable;
            renamecols = true
        )
    )
end

"""
    _calculate_state_seroprevalence(table, original_df)

An internal function to handle the calculation of the state/serotype counts based upon the provided state/serotype seroprevalence values and total state counts.
Because DataFrames handles tables as named tuples, we can extract information about the columns being passed from the regex selection and then use substitution strings to collect a view of the correct column of total state counts.
"""
function _calculate_state_seroprevalence(table, original_df)
    str_keys = String.(keys(table))
    timing = replace.(str_keys, r"serotype_.*_\(n\)_(\w+)$" => s"serotype_all_(n)_\1")
    vals = map(
        ((serotype_count, agg_counts_col),) -> (serotype_count ./ @view(original_df[!, agg_counts_col])) .* 100,
        zip(table, timing)
    )

    names = Symbol.(replace.(str_keys, r"(.*_)\(n\)(_.*)" => s"\1(%)\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end
