using DataFrames: DataFrame, DataFrameRow, select, select!, subset, filter, rename, rename!, transform, transform!, ByRow, Not, Cols, nrow, AsTable, ncol
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
        df::DataFrame,
        year::I;
        year_column = :sample_year
    ) where {I <: Integer}
    df[!, year_column] .= year
    return Try.Ok(nothing)
end

