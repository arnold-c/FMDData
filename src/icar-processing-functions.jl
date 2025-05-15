using DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, select, select!, subset, subset!, filter, rename, rename!, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict
using StatsBase: mean
using Try
using TryExperimental

export add_round_name!,
    add_report_year!,
    add_sample_year!,
    add_metadata_col!,
    infer_later_year_values!

function add_round_name!(df_round_pairs...; round_column = :round)
    return add_metadata_col!(round_column, df_round_pairs...)
end

function add_round_name!(
        df_round_pair::Pair{T, S};
        round_column = :round
    ) where {T <: AbstractDataFrame, S <: AbstractString}
    return add_metadata_col!(year_column, df_round_pair)
end

function add_report_year!(df_year_pairs...; year_column = :report_year)
    return add_metadata_col!(year_column, df_year_pairs...)
end

function add_report_year!(
        df_year_pair::Pair{T, I};
        year_column = :report_year
    ) where {T <: AbstractDataFrame, I <: Integer}
    return add_metadata_col!(year_column, df_year_pair)
end

function add_sample_year!(df_year_pairs...; year_column = :sample_year)
    return add_metadata_col!(year_column, df_year_pairs...)
end

function add_sample_year!(
        df_year_pair::Pair{T, I};
        year_column = :sample_year
    ) where {T <: AbstractDataFrame, I <: Integer}
    return add_metadata_col!(year_column, df_year_pair)
end

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

function add_metadata_col!(
        metadata_column::Symbol,
        df_metadata_pair::Pair{T, I},
    ) where {T <: AbstractDataFrame, I <: Union{<:Integer, <:AbstractFloat, <:AbstractString}}
    df, metadata = df_metadata_pair
    df[!, metadata_column] .= metadata
    return Try.Ok(nothing)
end

function infer_later_year_values!(
        later_df::T,
        initial_df::T;
        year_column = :sample_year,
        statename_column = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_count_(pre|post).*"
        ),
        digits = 1

    ) where {T <: AbstractDataFrame}
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

            ismissing(later_df[later_state_idx, col_name]) &&
                return Try.Err("State $state_name and column $col_name value is missing in the follow-up dataset.")

            initial_value = if ismissing(initial_df[initial_state_idx, col_name])
                convert(eltype(initial_df[!, col_name]), 0)
            else
                initial_df[initial_state_idx, col_name]
            end

            later_df[later_state_idx, col_name] = later_df[later_state_idx, col_name] - initial_value
        end
    end

    # Only calculate for count columns
    totals_dict = _log_try_error(calculate_all_totals(later_df; reg = reg))
    totals_check_state = all_totals_check(totals_dict, later_df; reg = reg)

    if Try.iserr(totals_check_state)
        push!(
            later_df,
            merge(Dict("states_ut" => "Total calculated"), totals_dict);
            promote = true,
            cols = :subset
        )
        select_calculated_totals!(later_df)
    end

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
    rename!(
        n -> replace(
            n,
            r"(.*_calculated)_calculated" => s"\1"
        ),
        later_df,
    )
    select_calculated_cols!(later_df)

    _remove_states_without_data!(
        later_df;
        reg = update_regex(
            reg,
            r"(.*)count(.*)",
            s"\1(?:count|pct)\2",

        )
    )

    return Try.Ok(nothing)
end

function _remove_states_without_data!(
        df;
        column::Symbol = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?:$(join(default_allowed_serotypes, "|")))_(?:count|pct)_(?:pre|post).*")
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
