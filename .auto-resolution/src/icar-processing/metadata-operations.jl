using DataFrames: AbstractDataFrame, DataFrame
using OrderedCollections: OrderedDict
using Try

export add_all_metadata!,
    add_test_threshold!,
    add_test_type!,
    add_round_name!,
    add_report_year!,
    add_sample_year!,
    add_metadata_col!


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
        !(k in acceptable_metadata) && push!(unaccepted_metadata, k => dict[k])
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