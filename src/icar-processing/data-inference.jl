using DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, select, select!, subset, subset!, filter, rename, rename!, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict
using StatsBase: mean
using Try
using TryExperimental

export infer_later_year_values


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

Infer single-year values by subtracting initial year data from cumulative data.

This function is used when ICAR reports provide cumulative data (e.g., 2021+2022 combined)
and you need to extract the data for just the later year (e.g., 2022 only).

# Arguments
- `cumulative_later_df`: DataFrame with cumulative data for multiple years
- `initial_df`: DataFrame with data for the initial year only
- `year_column`: Column containing sample year information
- `statename_column`: Column containing state/UT names
- `allowed_serotypes`: Vector of serotypes to process
- `reg`: Regex pattern to identify count columns for processing
- `atol`: Absolute tolerance for floating-point comparisons
- `digits`: Number of decimal places for calculated percentages

# Returns
- `Try.Ok(inferred_df)` with single-year data for the later period
- `Try.Err(message)` if inference fails

# Process
1. Subtracts initial year counts from cumulative counts
2. Handles rounding errors and missing values
3. Recalculates totals and seroprevalence percentages
4. Removes states with no data
5. Validates final totals
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


    @? all_totals_check(
        later_df;
        reg = count_pct_reg,
        atol = atol,
        digits = digits
    )

    return Try.Ok(later_df)
end

"""
    _remove_states_without_data!(
        df;
        column::Symbol = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex
	)

Internal function that removes states that do not contain any data.
"""
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
