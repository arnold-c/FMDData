using DrWatson: datadir, scriptsdir
using CSV: read, write
using DataFrames: DataFrame, DataFrameRow, select, subset, filter, rename, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict
using StatsBase: mean
using Try
using TryExperimental
using Logging
using LoggingExtras

export all_cleaning_steps,
    load_csv,
    clean_colnames,
    rename_aggregated_pre_post_counts,
    correct_all_state_names,
    check_duplicated_column_names,
    check_duplicated_columns,
    check_missing_states,
    check_duplicated_states,
    check_allowed_serotypes,
    check_seroprevalence_as_pct,
    check_aggregated_pre_post_counts_exist,
    check_pre_post_exists,
    has_totals_row,
    all_totals_check,
    calculate_all_totals,
    calculate_totals,
    totals_check,
    calculate_state_counts,
    calculate_state_seroprevalence,
    check_calculated_values_match_existing,
    select_calculated_totals!,
    select_calculated_cols!


public collect_all_present_serotypes,
    correct_state_name

default_allowed_serotypes::Vector{String} = ["o", "a", "asia1"]

function all_cleaning_steps(
        input_filename::T1,
        input_dir::T1,
        output_filename::T1 = "clean_$input_filename",
        output_dir::T1 = datadir("icar-seroprevalence");
        load_format = DataFrame
    ) where {T1 <: AbstractString}

    println("\n==========================================================================")
    println("Cleaning $(joinpath(input_dir, input_filename))\n")

    data = Try.@? load_csv(
        input_filename,
        input_dir,
        load_format
    )
    cleaned_colnames_data = Try.@? clean_colnames(data)
    renamed_aggregate_counts_data = Try.@? rename_aggregated_pre_post_counts(cleaned_colnames_data)
    corrected_state_name_data = Try.@? correct_all_state_names(renamed_aggregate_counts_data)

    Try.@? check_duplicated_column_names(corrected_state_name_data)
    Try.@? check_missing_states(corrected_state_name_data)
    Try.@? check_duplicated_states(corrected_state_name_data)
    Try.@? check_allowed_serotypes(corrected_state_name_data)
    Try.@? check_seroprevalence_as_pct(corrected_state_name_data)
    Try.@? check_aggregated_pre_post_counts_exist(corrected_state_name_data)
    Try.@? check_pre_post_exists(corrected_state_name_data)
    Try.@? has_totals_row(corrected_state_name_data)


    filebase = match(r"(.*)\.csv", input_filename).captures[1]
    logger = FileLogger(joinpath(output_dir, "logfiles", "$filebase.log"))

    function log_try_error(res)
        if Try.iserr(res)
            @error(Try.unwrap_err(res))
        end
        return nothing
    end

    with_logger(logger) do
        if Try.iserr(all_totals_check(corrected_state_name_data))
            log_try_error(all_totals_check(corrected_state_name_data))
            totals = Try.@? calculate_all_totals(corrected_state_name_data)
            push!(
                corrected_state_name_data,
                merge(Dict("states_ut" => "Total calculated"), totals);
                promote = true
            )
        end
    end

    calculated_state_counts_data = calculate_state_counts(corrected_state_name_data)
    calculated_state_seroprevs_data = calculate_state_seroprevalence(calculated_state_counts_data)

    Try.@? check_calculated_values_match_existing(calculated_state_seroprevs_data)

    Try.@? write_csv(output_filename, output_dir, calculated_state_seroprevs_data)

    return Try.Ok("Cleaning of $input_filename successful. Written to $output_filename.")
end

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
    isdir(dir) || return Err("$dir is not a valid directory")
    contains(filename, r".*\.csv$") || return Err("$filename is not a csv file")

    dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(dir))
    in(filename, dir_files) || return Err("$filename is not within the directory $dir")

    return Try.Ok(
        read(
            joinpath(dir, filename),
            output_format
        )
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

    length(cols_with_dissallowed_chars) == 0 || return Err("$(keys(cols_with_dissallowed_chars)) are columns with disallowed characters.\n$(cols_with_dissallowed_chars)")

    return Try.Ok(clean_df)
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
    return Try.Ok(
        rename(
            s -> replace(s, original_regex => substitution_string),
            df
        )
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

    return Try.Ok(
        transform(
            df,
            column => ByRow(s -> correct_state_name(s, states_dict));
            renamecols = false
        )
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
        return Try.Err("State name `$input_name` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling")

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
    identical_check = _check_identical_column_names(df)
    similar_check = _check_similar_column_names(df)
    if !Try.iserr(identical_check) && !Try.iserr(similar_check)
        return Ok(nothing)
    end
    return Try.Err(_combine_error_messages([identical_check, similar_check]))
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

    df_ncol == length(unique_colnames) || return Err("The dataframe has $df_ncol columns, but only $(length(unique_colnames)) unique column names. $(keys(filter(c -> values(c) != 1, colname_counts))) were duplicated.")

    return Try.Ok(nothing)
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
                return Err("Has duplicate names. Run the function _check_identical_column_names() before running this function.")
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
        return Err("Similar column names were found in the data: $(sort!(duplicates; by = first)).")
    end
    return Ok(nothing)
end

"""
    _combine_error_messages(arr_of_errs::AbstractVector{T}) where {T <: Try.InternalPrelude.AbstractResult}

Internal function that accepts a vector of `Try` results e.g., `Ok()` and `Err()`, and concatenates them to be passed up the call stack.
"""
function _combine_error_messages(arr_of_errs::AbstractVector{T}) where {T <: Try.InternalPrelude.AbstractResult}
    return String(
        strip(
            mapreduce(
                _unwrap_err_or_empty_str,
                (acc, next_val) -> acc * " " * next_val,
                arr_of_errs
            )
        )
    )
end

"""
    _unwrap_err_or_empty_str(res)

Internal function to check if result is an error and if so, return the unwrapped (error message) value. If the result is an Ok() result, return an empty string that will be used to during concatenation of error messages.
"""
function _unwrap_err_or_empty_str(res::Union{Ok{<:T}, Err{<:E}}) where {T <: AbstractString, E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return Try.unwrap(res)
end

function _unwrap_err_or_empty_str(res::Union{Ok{Nothing}, Err{<:E}}) where {E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return ""
end


"""
    check_duplicated_columns(df::DataFrame)

Check if any columns have identical values
"""
function check_duplicated_columns(df::DataFrame)
    df_ncol = ncol(df)
    df_ncol < 2 && return Try.Ok(nothing)

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
        return Try.Err(
            "Found columns with identical values: $(duplicated_columns)"
        )
    end
    return Try.Ok(nothing)
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
    nmissing == 0 || return Try.Err("There are $nmissing values in the $column column that are of type `Missing`")
    return Try.Ok(nothing)
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

    nstates == length(unique_states) || return Try.Err("The dataframe has $nstates state values, but only $(length(unique_states)) unique state values. $(String.(keys(filter(c -> values(c) != 1, state_counts)))) were duplicated")

    return Try.Ok(nothing)
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
    required_check = _check_all_required_serotypes(all_matched_serotypes, allowed_serotypes)
    disallowed_check = _check_no_disallowed_serotypes(all_matched_serotypes, allowed_serotypes)
    if !Try.iserr(required_check) && !Try.iserr(disallowed_check)
        return Ok(nothing)
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
    length(matched_serotypes) == length(allowed_serotypes) || return Try.Err("Found $(length(matched_serotypes)) allowed serotypes ($matched_serotypes). Required $(length(allowed_serotypes)): $allowed_serotypes.")
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
    length(matched_serotypes) == 0 || return Try.Err("Found $(length(matched_serotypes)) disallowed serotypes ($matched_serotypes).")
    return Try.Ok(nothing)

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

    missing_dict = OrderedDict{AbstractString, Vector{AbstractString}}()
    for serotype in unique_serotype_result
        pre_post_matches = filter(m -> m[1] == serotype, all_matched_columns)
        if length(unique(pre_post_matches)) != 2
            missing_dict[serotype] = map(m -> m[2], pre_post_matches)
        end
    end

    if !isempty(missing_dict)
        return Try.Err("All serotype results should have both 'Pre' and 'Post' results columns, only. Instead, the following serotype results have the associated data columns:\n$missing_dict")
    end

    return Try.Ok(nothing)
end

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
        if round(mean(skipmissing(vals)) / 100; digits = 1) < 0.1
            prop_cols_dict[name] = round(mean(skipmissing(vals)); digits = 2)
        end
    end
    if !isempty(prop_cols_dict)
        return Try.Err("All `pct` columns should be a %, not a proportion. The following columns are likely reported as proportions with associated mean values: $prop_cols_dict")
    end
    return Try.Ok(nothing)
end

"""
    check_aggregated_pre_post_counts_exist(
		df::DataFrame,
		columns = ["serotype_all_count_pre", "serotype_all_count_post"]
	)

Check if data contains aggregated counts of pre and post vaccinated individuals. Should only be used on dataframes that have renamed these columns to meet the standard pattern of "serotype_all_count_pre"
"""
function check_aggregated_pre_post_counts_exist(
        df::DataFrame,
        columns = ["serotype_all_count_pre", "serotype_all_count_post"]
    )
    sum(map(c -> in(c, names(df)), columns)) == length(columns) || return Try.Err("The aggregated count columns $columns do not exist in the data")
    return Try.Ok(nothing)
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
    length(filter(s -> in(s, possible_keys), lowercase.(df[!, column]))) > 0 || return Try.Err("Totals row not found in the data using the possible row keys $possible_keys in the column :$column")
    return Try.Ok(nothing)
end

"""
    all_totals_check(df::DataFrame, totals_key = "total")

Check if all provided values in the provided totals row are correct. If the column is a count, then calculate an unweighted sum. If the column is the seroprevalence, calculated the sum weighted by the relevant counts (pre- or post-vaccination counts).
"""
function all_totals_check(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$");
        atol = 0.1,
        digits = 1
    )

    totals_dict = Try.@? calculate_all_totals(
        df,
        column,
        totals_key,
        allowed_serotypes,
        reg;
        atol = atol,
        digits = digits
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
        column
    )

    return Try.Ok(nothing)
end

function calculate_all_totals(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total",
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$");
        atol = 0.1,
        digits = 1
    )
    totals_rn, selected_df = Try.@? _totals_row_selectors(
        df,
        column,
        totals_key;
        reg = reg
    )

    col_names = names(df)
    totals_dict = Dict{AbstractString, Real}()

    for col_ind in eachindex(names(selected_df))
        col_name = col_names[col_ind]
        totals_check_args = _collect_totals_check_args(
            selected_df[Not(totals_rn), col_ind],
            names(selected_df)[col_ind],
            selected_df,
            totals_rn,
            allowed_serotypes,
            atol,
        )
        Try.iserr(totals_check_args) && return totals_check_args
        _calculate_totals!(totals_dict, Try.unwrap(totals_check_args)...)
    end

    return Try.Ok(totals_dict)
end

function _totals_row_selectors(
        df::DataFrame,
        column::Symbol = :states_ut,
        totals_key = "total";
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg = Regex("serotype_(?|$(join(allowed_serotypes, "|")))_(count|pct)_(pre|post)\$")

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

Collect the necessary arguments to provide to the `totals_check()` functions. When checking the totals on counts, use `_...` varargs to denote additional arguments can be passed (necessary for total checks on seroprevalence values) but will not be assigned an used within the function body.

Returns a tuple of variables to be unpacked and passed to `totals_check()`
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
        atol = 0.1,
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

    return Try.Ok((col, colname, denom_col, denom_total, atol, digits))
end

function calculate_totals(
        col::Vector{T},
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}
    totals_dict = Dict{AbstractString, Real}()
    return _calculate_totals!(
        totals_dict,
        col,
        colname
    )
end

function _calculate_totals!(
        totals_dict::Dict,
        col::Vector{T},
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}
    calculated_total = sum(skipmissing(col))
    totals_dict[colname] = calculated_total
    return nothing
end

function calculate_totals(
        col::Vector{T},
        colname::String,
        denom_col::Vector{C},
        denom_total,
        atol = 0.1,
        digits = 1
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
    totals_dict = Dict{AbstractString, Real}()
    return _calculate_totals!(
        totals_dict,
        col,
        colname,
        denom_col,
        denom_total,
        atol,
        digits
    )
end

function _calculate_totals!(
        totals_dict::Dict,
        col::Vector{T},
        colname::String,
        denom_col::Vector{C},
        denom_total,
        atol = 0.1,
        digits = 1
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
    calculated_total = round(sum(skipmissing(col .* denom_col)) / denom_total; digits = digits)
    totals_dict[colname] = calculated_total
    return nothing
end

function totals_check(
        totals::DataFrameRow,
        calculated_totals::Dict,
        column::Symbol = :states_ut
    )
    errors_dict = OrderedDict{AbstractString, NamedTuple{(:provided, :calculated)}}()

    for colname in names(totals)
        provided_total = totals[colname]
        calculated_total = calculated_totals[colname]
        if provided_total != calculated_total
            errors_dict[colname] = (provided_total, calculated_total)
        end
    end

    if !isempty(errors_dict)
        return Try.Err("There were discrepancies in the totals calculated and those provided in the data: $errors_dict")
    end

    return Try.Ok(nothing)
end

"""
    totals_check(
        col::Vector{T},
        provided_total,
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}

Check if the provided total counts equal the sum calculated using the provided state counts.
"""
function _totals_check!(
        errors_dict::OrderedDict,
        col::Vector{T},
        provided_total,
        colname::String,
    ) where {T <: Union{<:Union{<:Missing, <:Integer}, <:Integer}}
    calculated_total = sum(skipmissing(col))
    if calculated_total != provided_total
        errors_dict[colname] = (provided_total, calculated_total)
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
function _totals_check!(
        errors_dict::OrderedDict,
        col::Vector{T},
        provided_total,
        colname::String,
        denom_col::Vector{C},
        denom_total,
        atol = 0.1,
        digits = 1
    ) where {
        T <: Union{<:Union{<:Missing, <:AbstractFloat}, <:AbstractFloat},
        C <: Union{<:Union{<:Missing, <:Integer}, <:Integer},
    }
    calculated_total = round(sum(skipmissing(col .* denom_col)) / denom_total; digits = digits)
    if !isapprox(calculated_total, provided_total; atol = atol)
        errors_dict[colname] = (provided_total, calculated_total)
    end
    return nothing
end


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

"""
    calculate_state_seroprevalence(df::DataFrame, allowed_serotypes = default_allowed_serotypes)

A wrapper function around the internal `_calculate_state_seroprevalence()` function to calculate the state/serotype specific counts based upon the state/serotype seroprevalence values and total state counts. See the documentation of `_calculate_state_seroprevalence()` for more details on the implementation.
"""
function calculate_state_seroprevalence(
        df::DataFrame,
        allowed_serotypes::T = default_allowed_serotypes;
        digits = 1
    ) where {T <: AbstractVector{<:AbstractString}}
    reg = Regex("serotype_($(join(allowed_serotypes, "|")))_count_(pre|post)\$")
    return hcat(
        df,
        select(
            df,
            AsTable(Cols(reg)) .=> (t -> _calculate_state_seroprevalence(t, df; digits = 1)) => AsTable;
            renamecols = true
        )
    )
end

"""
    _calculate_state_seroprevalence(table, original_df)

An internal function to handle the calculation of the state/serotype counts based upon the provided state/serotype seroprevalence values and total state counts.
Because DataFrames handles tables as named tuples, we can extract information about the columns being passed from the regex selection and then use substitution strings to collect a view of the correct column of total state counts.
"""
function _calculate_state_seroprevalence(table, original_df; digits = 1)
    str_keys = String.(keys(table))
    timing = replace.(str_keys, r"serotype_.*_count_(pre|post)$" => s"serotype_all_count_\1")
    vals = map(
        ((serotype_count, agg_counts_col),) -> round.((serotype_count ./ @view(original_df[!, agg_counts_col])) .* 100; digits = digits),
        zip(table, timing)
    )

    names = Symbol.(replace.(str_keys, r"(.*_)count(_.*)" => s"\1pct\2_calculated"))
    return NamedTuple{tuple(names...)}((vals...,))
end

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


    df[calculated_totals_rn, column] = titlecase("Total")
    popat!(df, provided_totals_rn)

    return Try.Ok(nothing)
end

"""
	select_calculated_cols!(
        df::DataFrame,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex
    )

Checks if the data contains both provided and calculated columns that refer to the same variables. If the calculated column is a % seroprevalence, keep the calculated values. If the calculated column is a column of counts, keep the provided as they are deemed to be more accurate (counts require no calculation and should be a direct recording/reporting of the underlying data). The cleaning function `check_calculated_values_match_existing()` should have been run before to ensure there are no surprises during this processing step i.e., accidentally deleting columns that should be retained.
"""
function select_calculated_cols!(
        df::DataFrame,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_(count|pct)_(?:pre|post)\$"
        )
    )
    colnames = names(df)
    all_matched_cols = filter(!isnothing, match.(reg, colnames))

    for col in all_matched_cols
        colnm = col.match
        colcap = col.captures

        length(colcap) == 1 || return Try.Err("Only 1 capture group should exist for the match $colnm. Found $(length(colcap)): $colcap.")
        colcap = colcap[1]
        colcap in ["count", "pct"] || return Try.Err("The capture group is not expected. It should be one of [\"count\", \"pct\"], but instead it is $colcap")

        calculated_col = colnm * "_calculated"
        calculated_present = calculated_col in colnames

        if calculated_present && colcap == "count"
            select!(df, Not(calculated_col))
        end

        if calculated_present && colcap == "pct"
            select!(df, Not(colnm))
            rename!(df, calculated_col => colnm)
        end
    end

    return Try.Ok(nothing)
end

function write_csv(
        filename::T1,
        dir::T1,
        data::DataFrame
    ) where {T1 <: AbstractString}
    isdir(dir) || return Err("$dir is not a valid directory")
    contains(filename, r".*\.csv$") || return Err("$filename is not a csv file")

    write(
        joinpath(dir, filename),
        data
    )
    return Try.Ok(nothing)
end
