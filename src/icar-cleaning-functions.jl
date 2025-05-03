using CSV: read
using DataFrames: DataFrame, select, subset, filter, rename, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict

export load_csv,
    clean_colnames,
    rename_aggregated_pre_post_counts,
    correct_all_state_names,
    check_duplicated_column_names,
    check_duplicated_columns,
    check_missing_states,
    check_duplicated_states,
    check_allowed_serotypes,
    check_pre_post_exists,
    has_totals_row,
    all_totals_check,
    calculate_state_counts,
    calculate_state_seroprevalence

public collect_all_present_serotypes,
    check_aggregated_pre_post_counts_exist,
    contains_seroprev_results,
    contains_count_results,
    correct_state_name

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
    clean_colnames(df::DataFrame, allowed_chars_reg::Regex)

Replace spaces and / with underscores, and (n) and (%) with "count" and "pct" respectively. `allowed_chars_reg` should be a negative match, where the default `r"[^\\w]"` matches to all non numeric/alphabetic/_ characters
"""
function clean_colnames(
        df::DataFrame,
        allowed_chars_reg::Regex = r"[^\w]"
    )
    clean_df = rename(
        t -> replace(
            lowercase(t),
            "/" => "_",
            " " => "_",
            "(n)" => "count",
            "_n_" => "_count_",
            "(%)" => "pct",
            "_%_" => "_pct_",
        ),
        df
    )

    colnames = names(clean_df)

    cols_with_dissallowed_chars = Dict(
        n => collect(eachmatch(allowed_chars_reg, n)) for n in colnames
    )

    filter!(
        c -> !isempty(c.second),
        cols_with_dissallowed_chars
    )

    @assert length(cols_with_dissallowed_chars) == 0 "$(keys(cols_with_dissallowed_chars)) are columns with disallowed characters.\n$(cols_with_dissallowed_chars)"

    return clean_df
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
        original_regex::Regex = r"^(pre|post)_count",
        substitution_string::SubstitutionString = s"serotype_all_count_\1"
    )
    return rename(
        s -> replace(s, original_regex => substitution_string),
        df
    )
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
        input_name::S,
        states_dict::Dict = FMDData.states_dict
    ) where {S <: AbstractString}
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
    check_duplicated_column_names(
        df::DataFrame,
        metric::T = Levenshtein();
        min_score = 0.79
    ) where {T <: Union{<:Metric, <:SemiMetric}}

Wrapper function around the two internal functions `_check_identical_column_names()` and `_check_similar_column_names()`. If a DataFrame is created then all identical column names should result in an error before it is created, but potentially they may be coerced to be made unique so a similarity check should be performed.
"""
function check_duplicated_column_names(df::DataFrame)
    _check_identical_column_names(df)
    _check_similar_column_names(df)
    return nothing
end

"""
    _check_identical_column_names(df::DataFrame)

Check if the provided data has any duplicate column names.

Should be run BEFORE `_check_similar_column_names()` as `push!()` call in `_check_similar_column_names` will overwrite previous Dict entry key (of similar column names) if there are exact matches.
"""
function _check_identical_column_names(df::DataFrame)
    df_ncol = ncol(df)
    colnames = String.(names(df))
    unique_colnames = unique(colnames)

    colname_counts = _calculate_string_occurences(colnames, unique_colnames)

    @assert df_ncol == length(unique_colnames) "The dataframe has $df_ncol columns, but only $(length(unique_colnames)) unique column names. $(keys(filter(c -> values(c) != 1, colname_counts))) were duplicated"

    return nothing
end

function _calculate_string_occurences(
        vals::Vector{S},
        unique_vals::Vector{S} = unique(vals)
    ) where {S <: AbstractString}
    return NamedTuple{tuple(Symbol.(unique_vals)...)}(
        map(
            i -> sum(i .== vals),
            unique_vals,
        )
    )
end

"""
    _check_similar_column_names(df::DataFrame) where {T <: Union{<:Metric, <:SemiMetric}}

Check if any columns have similar names. Calculates if any column names are substrings of other columns names.

Should be run AFTER `_check_identical_column_names()` as `push!()` call will overwrite previous Dict entry key if there are exact matches.
"""
function _check_similar_column_names(df::DataFrame)
    colnames = sort(String.(names(df)); by = length)
    duplicates = OrderedDict{String, Vector{String}}()
    for (i, nm) in pairs(colnames)
        for (_, next_nm) in pairs(colnames[(i + 1):end])
            if nm == next_nm
                error("Has duplicate names. Run the function _check_identical_column_names() before running this function")
            end
            if contains(next_nm, nm)
                if haskey(duplicates, nm)
                    push!(duplicates[nm], next_nm)
                else
                    duplicates[nm] = [next_nm]
                end
            end
            if contains(nm, next_nm)
                if haskey(duplicates, next_nm)
                    push!(duplicates[next_nm], nm)
                else
                    duplicates[next_nm] = [nm]
                end
            end
        end
    end

    for k in keys(duplicates)
        if in(k, reduce(vcat, values(duplicates)))
            pop!(duplicates, k)
        end
    end
    if !isempty(duplicates)
        error("Similar column names were found in the data:\n$(sort!(duplicates; by = first))")
    end
    return nothing
end

"""
    check_duplicated_columns(df::DataFrame)

Check if any columns have identical values
"""
function check_duplicated_columns(df::DataFrame)
    df_ncol = ncol(df)
    df_ncol < 2 && return nothing

    duplicate_columns_dict = Dict{AbstractVector, AbstractVector}()
    for (k, v) in pairs(eachcol(df))
        if haskey(duplicate_columns_dict, v)
            push!(duplicate_columns_dict[v], k)
        else
            duplicate_columns_dict[v] = [k]
        end
    end
    filter!(cols -> length(cols.second) > 1, duplicate_columns_dict)
    if !isempty(duplicate_columns_dict)
        duplicated_columns = [v for (_, v) in pairs(duplicate_columns_dict)]
        error(
            "Found columns with identical values: $(duplicated_columns)"
        )
    end
    return nothing
end

"""
    check_missing_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )

Check if the states column of the data contains missing values
"""
function check_missing_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )
    nmissing = sum(ismissing.(df[!, column]))
    return @assert nmissing == 0 "There are $nmissing values in the $column column that are of type `Missing`"
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
    )
    states = filter(!ismissing, df[!, column])
    nstates = length(states)
    unique_states = unique(states)
    state_counts = _calculate_string_occurences(states, unique_states)

    @assert nstates == length(unique_states) "The dataframe has $nstates state values, but only $(length(unique_states)) unique state values. $(String.(keys(filter(c -> values(c) != 1, state_counts)))) were duplicated"

    return nothing
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
        reg::Regex = r"serotype_(.*)_(?|count|pct)_(pre|post)"
    )
    all_matched_serotypes = unique(collect_all_present_serotypes(df, reg))
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
function check_pre_post_exists(
        df::DataFrame,
        reg::Regex = r"(serotype_.*_(?|count|pct))_(pre|post)"
    )
    colnames = names(df)

    all_matched_columns = filter(
        !isnothing,
        match.(reg, colnames)
    )

    unique_serotype_result = unique(map(m -> m[1], all_matched_columns))

    for serotype in unique_serotype_result
        pre_post_matches = filter(m -> m[1] == serotype, all_matched_columns)
        @assert length(unique(pre_post_matches)) == 2 "Serotype results $(serotype) should have both a 'Pre' and 'Post' results column. Instead, data contains $(length(unique(pre_post_matches))) columns: $(map(m -> m[2], pre_post_matches))"
    end

    return nothing
end

"""
    check_aggregated_pre_post_counts(
        df::DataFrame,
    )

Check if data contains aggregated counts of pre and post vaccinated individuals. Should only be used on dataframes that haven't yet renamed these columns to meet the standard pattern of "serotype_all_count_pre"
"""
function check_aggregated_pre_post_counts_exist(df::DataFrame, columns = ["pre_count", "post_count"])
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
    # Forms the regex string: r"serotype_(?|o|a|asia1)_pct_(pre|post)$"
    # (?|...) indicates a non-capture group i.e. must match any of the words separated by '|' characters, but does not return a match as a capture group
    # (pre|post) is the only capture group, providing the timing used to collect the correct state column for weighting the seroprevalence sums
    reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_pct_(pre|post)\$")
    denom_type_matches = match(reg, colname)
    @assert length(denom_type_matches) == 1 "For column $colname, $(length(denom_type_matches)) possible denominators found, but only expected 1: $(denom_type_matches.captures)"
    denom_type = denom_type_matches[1]
    denom_colname = "serotype_all_count_$denom_type"

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
    calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

A wrapper function around the internal `_calculate_state_counts()` function to calculate the state/serotype specific counts based upon the state/serotype seroprevalence values and total state counts. See the documentation of `_calculate_state_counts()` for more details on the implementation.
"""
function calculate_state_counts(df::DataFrame, allowed_serotypes = default_allowed_serotypes)
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
        original_view = @view(original_df[!, agg_counts_col])
        vals = round.((seroprev / 100) .* original_view)
        return convert.(eltype(original_view), vals)
    end

    names = Symbol.(replace.(str_keys, r"(.*_)pct(_.*)" => s"\1count\2_calculated"))
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
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_count_(pre|post)\$")
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
    timing = replace.(str_keys, r"serotype_.*_count_(pre|post)$" => s"serotype_all_count_\1")
    vals = map(
        ((serotype_count, agg_counts_col),) -> (serotype_count ./ @view(original_df[!, agg_counts_col])) .* 100,
        zip(table, timing)
    )

    names = Symbol.(replace.(str_keys, r"(.*_)count(_.*)" => s"\1pct\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end
