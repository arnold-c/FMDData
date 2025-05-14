using DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, select, select!, subset, filter, rename, rename!, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
using OrderedCollections: OrderedDict
using StatsBase: mean
using Try
using TryExperimental

export add_round_name!,
    add_report_year!,
    add_sample_year!

function add_round_name!(
        df::DataFrame,
        round_name::String,
        round_name_column::Symbol = :round
    )
    df[!, round_name_column] .= round_name
    return Try.Ok(nothing)
end
function add_report_year!(
        df::DataFrame,
        year::I;
        year_column = :report_year
    ) where {I <: Integer}
    df[!, year_column] .= year
    return Try.Ok(nothing)
end

function add_sample_year!(df_year_pairs...)
    sample_year_errs = OrderedDict()
    for pair in df_year_pairs
        res = add_sample_year!(pair)
        if Try.iserr(res)
            push!(sample_year_errs, res)
        end
    end
    if !isempty(sample_year_errs)
        Try.Err(_combine_error_messages(sample_year_errs))
    end
    return Try.Ok(nothing)
end

function add_sample_year!(
        df_year_pair::Pair{T, I};
        year_column = :sample_year
    ) where {T <: AbstractDataFrame, I <: Integer}
    df, year = df_year_pair
    df[!, year_column] .= year
    return Try.Ok(nothing)
end

function add_sample_year!(
        later_df_year_pair::Pair{T, I},
        initial_df_year_pair::Pair{T, I};
        year_column = :sample_year,
        statename_column = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_count_(pre|post).*"
        ),
        digits = 1

    ) where {T <: AbstractDataFrame, I <: Integer}
    initial_df, initial_year = initial_df_year_pair
    later_df, later_year = later_df_year_pair

    initial_year < later_year || return Try.Err("The initial year provided ($initial_year) is not before the later year ($later_year)")

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
    pct_reg = Regex(
        replace(
            reg.pattern,
            r"(.*)all|(.*)" => s"\1\2",
        )
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

    initial_df[!, year_column] .= initial_year
    later_df[!, year_column] .= later_year


    # transform!(
    #     later_df,
    #     Cols(pct_reg) => ByRow(p => replace(p, NaN => missing))
    # )


    return Try.Ok(nothing)
end

function _remove_states_without_data!(
        df;
        column::Symbol = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex("serotype_(?|$(join(default_allowed_serotypes, "|")))_(?|count|pct)_(?|pre|post).*")
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
