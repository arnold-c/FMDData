using DataFrames: DataFrame
using OrderedCollections: OrderedDict
using Try: Try

export check_duplicated_column_names,
    check_duplicated_columns


"""
    check_duplicated_column_names(
        df::DataFrame,
        metric::T = Levenshtein();
        min_score = 0.79
    ) where {T <: Union{<:Metric, <:SemiMetric}}

Wrapper function around the two internal functions [`_check_identical_column_names()`](@ref) and [`_check_similar_column_names()`](@ref). If a DataFrame is created then all identical column names should result in an error before it is created, but potentially they may be coerced to be made unique so a similarity check should be performed.
"""
function check_duplicated_column_names(df::DataFrame)
    identical_check = _check_identical_column_names(df)
    similar_check = _check_similar_column_names(df)
    if !Try.iserr(identical_check) && !Try.iserr(similar_check)
        return Try.Ok(nothing)
    end
    return Try.Err(_combine_error_messages([identical_check, similar_check]))
end

"""
    _check_identical_column_names(df::DataFrame)

Check if the provided data has any duplicate column names.

Should be run BEFORE [`_check_similar_column_names()`](@ref) as `push!()` call in [`_check_similar_column_names`](@ref) will overwrite previous Dict entry key (of similar column names) if there are exact matches.
"""
function _check_identical_column_names(df::DataFrame)
    df_ncol = ncol(df)
    colnames = String.(names(df))
    unique_colnames = unique(colnames)

    colname_counts = _calculate_string_occurences(colnames, unique_colnames)

    df_ncol == length(unique_colnames) ||
        return Try.Err("The dataframe has $df_ncol columns, but only $(length(unique_colnames)) unique column names. $(keys(filter(c -> values(c) != 1, colname_counts))) were duplicated.")

    return Try.Ok(nothing)
end


"""
    _check_similar_column_names(df::DataFrame) where {T <: Union{<:Metric, <:SemiMetric}}

Check if any columns have similar names. Calculates if any column names are substrings of other columns names.

Should be run AFTER [`_check_identical_column_names()`](@ref) as `push!()` call will overwrite previous Dict entry key if there are exact matches.
"""
function _check_similar_column_names(df::DataFrame)
    colnames = sort(String.(names(df)); by = length)
    duplicates = OrderedDict{String, Vector{String}}()
    for (i, nm) in pairs(colnames)
        for (_, next_nm) in pairs(colnames[(i + 1):end])
            if nm == next_nm
                return Try.Err("Has duplicate names. Run the function _check_identical_column_names() before running this function.")
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
        return Try.Err("Similar column names were found in the data: $(sort!(duplicates; by = first)).")
    end
    return Try.Ok(nothing)
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
