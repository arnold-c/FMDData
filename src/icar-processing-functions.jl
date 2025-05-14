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

    later_colnames = names(later_df)
    initial_colnames = names(initial_df)
    common_colnames = intersect(later_colnames, initial_colnames)
    common_states = intersect(
        later_df[!, statename_column],
        initial_df[!, statename_column]
    )

    initial_df[!, year_column] .= initial_year
    later_df[!, year_column] .= later_year

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

    pct_reg = Regex(
        replace(
            reg.pattern,
            r"(.*)all|(.*)" => s"\1\2",
        )
    )
    @show pct_reg

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

    return Try.Ok(nothing)
end
