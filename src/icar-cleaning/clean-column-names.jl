using DataFrames: DataFrame, rename
using Try: Try

export clean_colnames,
    rename_aggregated_pre_post_counts
"""
    clean_colnames(df::DataFrame, allowed_chars_reg::Regex)

Replace spaces and / with underscores, and (n) and (%) with "count" and "pct" respectively. `allowed_chars_reg` should be a negative match, where the default `r"[^\\w]"` matches to all non numeric/alphabetic/_ characters
"""
function clean_colnames(
        df::DataFrame,
        allowed_chars_reg::Regex = r"[^\w]",
    )
    clean_df = rename(
        t -> replace(
            lowercase(t),
            "/" => "_",
            " " => "_",
            "(n)" => "count",
            "_n_" => "_count_",
            "(%)" => "pct",
            "_%_" => "_pct_",
        ),
        df
    )

    colnames = names(clean_df)

    cols_with_dissallowed_chars = Dict(
        n => collect(eachmatch(allowed_chars_reg, n)) for n in colnames
    )

    filter!(
        c -> !isempty(c.second),
        cols_with_dissallowed_chars
    )

    length(cols_with_dissallowed_chars) == 0 ||
        return Try.Err("$(keys(cols_with_dissallowed_chars)) are columns with disallowed characters.\n$(cols_with_dissallowed_chars)")

    return Try.Ok(clean_df)
end

"""
    rename_aggregated_pre_post_counts(
        df::DataFrame,
        original_regex::Regex
        substitution_string::SubstitutionString
    )

Rename the aggregated pre/post counts to use the same format as the serotype-specific values
"""
function rename_aggregated_pre_post_counts(
        df::DataFrame,
        original_regex::Regex = r"^(pre|post)_count",
        substitution_string::SubstitutionString = s"serotype_all_count_\1",
    )
    return Try.Ok(
        rename(
            s -> replace(s, original_regex => substitution_string),
            df
        )
    )
end
