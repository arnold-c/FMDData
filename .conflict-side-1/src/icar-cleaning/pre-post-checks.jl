using DataFrames: DataFrame
using OrderedCollections: OrderedDict
using Try: Try

export check_pre_post_exists,
    check_aggregated_pre_post_counts_exist

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
    sum(map(c -> in(c, names(df)), columns)) == length(columns) ||
        return Try.Err("The aggregated count columns $columns do not exist in the data")
    return Try.Ok(nothing)
end
