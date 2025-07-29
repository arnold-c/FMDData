using DataFrames: DataFrame, select!, sort!
using Try: Try

export sort_columns!,
    sort_states!

"""
	sort_columns!(
        df::DataFrame;
        statename_column = :states_ut,
        allowed_serotypes = vcat("all", default_allowed_serotypes)
        prefix = "serotype_",
        suffix_order = [
            "_count_pre",
            "_pct_pre",
            "_count_post",
            "_pct_post",
        ]
    )

Sort the columns of the cleaned dataframe to have a consistent order. Follows the pattern:

- state column name
- serotype all counts (pre then post)
- serotype specific columns in the order "O", "A", "Asia1"

The serotype specific columns have their data presented in the following order.
- serotype X pre count
- serotype X pre pct
- serotype X post count
- serotype X post pct
"""
function sort_columns!(
        df::DataFrame;
        statename_column = :states_ut,
        region_code_column = :region_code,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        prefix = "serotype_",
        suffix_order = [
            "_count_pre",
            "_pct_pre",
            "_count_post",
            "_pct_post",
        ]
    )

    colnames = names(df)

    ordered_names = String.([statename_column, region_code_column])

    for serotype in allowed_serotypes
        for suffix in suffix_order
            colname = prefix * serotype * suffix
            push!(ordered_names, colname)
        end
    end

    missed_colnames = setdiff(colnames, ordered_names)

    if !isempty(missed_colnames)
        ordered_names = vcat(ordered_names, missed_colnames)
    end

    ordered_names = intersect(ordered_names, colnames)

    select!(df, ordered_names)

    return Try.Ok(nothing)
end

"""
	sort_states!(
        df::DataFrame;
        statename_column = :states_ut,
        roundname_column = :round_name,
        totals_key = "total"
    )

Sort the dataframe by alphabetical order of the states and list the totals row at the bottom. Preserves the original order of rows if there are duplicates.
"""
function sort_states!(
        df::DataFrame;
        statename_column = :states_ut,
        roundname_column = :round_name,
        totals_key = "total"
    )
    statename_fn = n -> (lowercase(n) == totals_key, n)
    if roundname_column in names(df)
        sort(
            df,
            [statename_column, roundname_column],
            by = [statename_fn, identity]
        )
    else
        sort(
            df,
            [statename_column],
            by = [statename_fn]
        )
    end
    return Try.Ok(nothing)
end
