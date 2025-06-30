using DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, select, select!, subset, subset!, filter, rename, rename!, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict
using StatsBase: mean
using Try
using TryExperimental

export add_all_metadata!,
    add_test_threshold!,
    add_test_type!,
    add_round_name!,
    add_report_year!,
    add_sample_year!,
    add_metadata_col!,
    infer_later_year_values,
    combine_round_dfs


"""
    add_all_metadata!(
        df_pair::Pair{T, D}
    ) where {T <: DataFrame, D <: OrderedDict{<:Symbol, <:Any}}

Adds multiple metadata columns to a DataFrame based on a dictionary of metadata.

# Arguments
- `df_pair`: A `Pair` where the key is the DataFrame to modify and the value is an `OrderedDict` of metadata. The keys of the dictionary should be the names of the metadata columns to add, and the values should be the values to populate those columns with.
"""
function add_all_metadata!(
        df_pair::Pair{T, D}
    ) where {T <: DataFrame, D <: OrderedDict{<:Symbol, <:Any}}

    df, dict = df_pair

    acceptable_metadata = (
        :sample_year,
        :report_year,
        :round_name,
        :test_type,
        :test_threshold,
    )

    unaccepted_metadata = OrderedDict()
    for k in keys(dict)
        !(k in acceptable_metadata) && push!(unaccepted_metadata, k)
    end
    isempty(unaccepted_metadata) ||
        return Try.Err("Metadata provided that is not accepted: $unaccepted_metadata")

    for metadata in acceptable_metadata
        if haskey(dict, metadata)
            @? add_metadata_col!(metadata, df => dict[metadata])
        end
    end

    return Try.Ok(nothing)
end


"""
    add_test_threshold!(
        df_round_pairs::Pair{T, S}...;
        threshold_column = :test_threshold
    ) where {T <: AbstractDataFrame, S <: AbstractString}

Adds a test threshold column to one or more DataFrames.
"""
function add_test_threshold!(
        df_round_pairs::Pair{T, S}...;
        threshold_column = :test_threshold
    ) where {T <: AbstractDataFrame, S <: AbstractString}
    return add_metadata_col!(threshold_column, df_round_pairs...)
end


"""
    add_test_type!(
        df_round_pairs::Pair{T, S}...;
        test_column = :test_type
    ) where {T <: AbstractDataFrame, S <: AbstractString}

Adds a test type column to one or more DataFrames.
"""
function add_test_type!(
        df_round_pairs::Pair{T, S}...;
        test_column = :test_type
    ) where {T <: AbstractDataFrame, S <: AbstractString}
    return add_metadata_col!(test_column, df_round_pairs...)
end


"""
    add_round_name!(
        df_round_pairs::Pair{T, S}...;
        round_column = :round
    ) where {T <: AbstractDataFrame, S <: AbstractString}

Adds a round name column to one or more DataFrames.
"""
function add_round_name!(
        df_round_pairs::Pair{T, S}...;
        round_column = :round
    ) where {T <: AbstractDataFrame, S <: AbstractString}
    return add_metadata_col!(round_column, df_round_pairs...)
end


"""
    add_report_year!(
        df_year_pairs::Pair{T, I}...;
        year_column = :report_year
    ) where {T <: AbstractDataFrame, I <: Integer}

Adds a report year column to one or more DataFrames.
"""
function add_report_year!(
        df_year_pairs::Pair{T, I}...;
        year_column = :report_year
    ) where {T <: AbstractDataFrame, I <: Integer}
    return add_metadata_col!(year_column, df_year_pairs...)
end


"""
    add_sample_year!(
        df_year_pairs...;
        year_column = :sample_year
    )

Adds a sample year column to one or more DataFrames.
"""
function add_sample_year!(
        df_year_pairs...;
        year_column = :sample_year
    )
    return add_metadata_col!(year_column, df_year_pairs...)
end

"""
    add_metadata_col!(metadata_column, df_metadata_pairs...)

Adds a metadata column to one or more DataFrames. This is a generic function that can be used to add any metadata column.
"""
function add_metadata_col!(metadata_column, df_metadata_pairs...)
    metadata_errs = OrderedDict()
    for pair in df_metadata_pairs
        res = add_metadata_col!(metadata_column, pair)
        if Try.iserr(res)
            push!(metadata_errs, res)
        end
    end
    if !isempty(metadata_errs)
        Try.Err(_combine_error_messages(metadata_errs))
    end
    return Try.Ok(nothing)
end

"""
    add_metadata_col!(
        metadata_column::Symbol,
        df_metadata_pair::Pair{T, I},
    ) where {T <: AbstractDataFrame, I <: Union{<:Integer, <:AbstractFloat, <:AbstractString}}

Adds a metadata column to a single DataFrame.
"""
function add_metadata_col!(
        metadata_column::Symbol,
        df_metadata_pair::Pair{T, I},
    ) where {T <: AbstractDataFrame, I <: Union{<:Integer, <:AbstractFloat, <:AbstractString}}
    df, metadata = df_metadata_pair
    df[!, metadata_column] .= metadata
    return Try.Ok(nothing)
end


"""
    infer_later_year_values(
        cumulative_later_df::T,
        initial_df::T;
        year_column = :sample_year,
        statename_column = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex,
        atol = 0.0,
        digits = 1

    ) where {T <: AbstractDataFrame}

Infers the values for a later year by subtracting the values from an initial year from a cumulative dataset. This is useful when a report provides cumulative data, and you need to extract the data for a single year.
"""
function infer_later_year_values(
        cumulative_later_df::T,
        initial_df::T;
        year_column = :sample_year,
        statename_column = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_count_(pre|post).*"
        ),
        atol = 0.0,
        digits = 1

    ) where {T <: AbstractDataFrame}
    later_df = deepcopy(cumulative_later_df)
    later_colnames = names(later_df)
    initial_colnames = names(initial_df)
    common_colnames = intersect(later_colnames, initial_colnames)
    common_states = intersect(
        later_df[!, statename_column],
        initial_df[!, statename_column]
    )

    common_count_colnames = filter(s -> contains(s, reg), common_colnames)

    for col_name in common_count_colnames
        for state_name in common_states
            later_state_idx = findfirst(
                s -> s .== state_name,
                later_df[!, statename_column]
            )
            initial_state_idx = findfirst(
                s -> s .== state_name,
                initial_df[!, statename_column]
            )

            ismissing(later_df[later_state_idx, col_name]) && !ismissing(initial_df[initial_state_idx, col_name]) &&
                return Try.Err("State $state_name and column $col_name value is missing in the follow-up dataset, but not in the initial dataset.")

            initial_value = if ismissing(initial_df[initial_state_idx, col_name])
                convert(eltype(initial_df[!, col_name]), 0)
            else
                initial_df[initial_state_idx, col_name]
            end
            later_df[later_state_idx, col_name] = later_df[later_state_idx, col_name] - initial_value

            # if the initial value is smaller than the later value by 1 then it's due to a rounding issue in calculating the counts from a pct
            if !ismissing(later_df[later_state_idx, col_name]) && later_df[later_state_idx, col_name] == -1
                later_df[later_state_idx, col_name] = 0
            end
        end
    end

    _correct_serotype_counts!(later_df; reg = reg)

    # Only calculate for count columns
    totals_dict = @? calculate_all_totals(later_df; reg = reg)
    push!(
        later_df,
        merge(Dict("states_ut" => "Total calculated"), totals_dict);
        promote = true,
        cols = :subset
    )
    select_calculated_totals!(later_df)


    # Calculate state serotype pct values
    pct_reg = update_regex(
        reg,
        r"(.*)all|(.*)",
        s"\1\2",
    )

    transform!(
        later_df,
        AsTable(Cols(pct_reg)) .=> (
            t -> _calculate_state_seroprevalence(
                t,
                later_df;
                reg = pct_reg,
                digits = digits
            )
        ) => AsTable;
        renamecols = true
    )

    select_calculated_cols!(later_df)

    count_pct_reg = update_regex(
        reg,
        r"(.*)count(.*)",
        s"\1(?:count|pct)\2",
    )

    _remove_states_without_data!(
        later_df;
        reg = count_pct_reg
    )

    @? sort_states!(later_df)
    @? sort_columns!(later_df)


    @? all_totals_check(later_df; reg = count_pct_reg, atol = atol, digits = digits)

    return Try.Ok(later_df)
end

function _remove_states_without_data!(
        df;
        column::Symbol = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_(?:count|pct)_(?:pre|post).*"
        )
    )
    states = String[]
    for row in eachrow(df)
        state = row[column]
        row_total = sum(skip_missing_and_nan(row[Cols(reg)]))
        if row_total == 0.0
            push!(states, state)
        end
    end
    subset!(
        df,
        column => ByRow(c -> !(c in states)),
    )
    return nothing
end

"""
	_correct_serotype_counts!(
        df::DataFrame;
        statename_column = :states_ut,
        allowed_serotypes = default_allowed_serotypes,
        reg::Regex
	)

Correct any serotype counts that have been miscalculated during the inferral steps, arising from rounding errors in the provided seroprevalence numbers that are then translated into counts to difference between initial and later dataframes. If the pre or post counts for all serotypes are 0, then all serotype specific counts must be 0 as well, so correct.
"""
function _correct_serotype_counts!(
        df::DataFrame;
        statename_column = :states_ut,
        allowed_serotypes = default_allowed_serotypes,
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_(count)_(?:pre|post)\$"
        ),
    )

    all_count_reg = update_regex(
        reg,
        r"(serotype_)(?:.*)(_\(count\).*)",
        s"\1all\2"
    )


    pre_all_column = df[!, all_count_reg]

    for (nm, col) in pairs(eachcol(pre_all_column))
        pre_post_type = match(r".*(pre|post)", String(nm))[1]
        pre_post_serotype_count_reg = update_regex(
            reg,
            r"(.*)\(.*pre\|post\).*",
            SubstitutionString("\\1(?:$(pre_post_type))")
        )

        pre_post_edit_idx = []
        for (i, v) in pairs(col)
            if ismissing(v) || v == 0
                push!(pre_post_edit_idx, i)
            end
        end
        if !isempty(pre_post_edit_idx)
            df[pre_post_edit_idx, pre_post_serotype_count_reg] .= 0
        end
    end

    return df
end


"""
    combine_round_dfs(dfs::DataFrame...)

Combines multiple DataFrames into a single DataFrame.
"""
function combine_round_dfs(
        dfs::DataFrame...
    )
    return Try.Ok(vcat(dfs...))
end
