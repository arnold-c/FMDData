using DataFrames: DataFrame, Not, select!, rename!
using Try: Try

export select_calculated_cols!

"""
	select_calculated_cols!(
        df::DataFrame,
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex
    )

Checks if the data contains both provided and calculated columns that refer to the same variables. If the calculated column is a % seroprevalence, keep the calculated values. If the calculated column is a column of counts, keep the provided as they are deemed to be more accurate (counts require no calculation and should be a direct recording/reporting of the underlying data). The cleaning function `check_calculated_values_match_existing()` should have been run before to ensure there are no surprises during this processing step i.e., accidentally deleting columns that should be retained.
"""
function select_calculated_cols!(
        df::DataFrame;
        allowed_serotypes = vcat("all", default_allowed_serotypes),
        reg::Regex = Regex(
            "serotype_(?:$(join(allowed_serotypes, "|")))_(count|pct)_(?:pre|post)\$"
        )
    )
    colnames = names(df)
    all_matched_cols = filter(!isnothing, match.(reg, colnames))

    length(all_matched_cols) > 0 ||
        return Try.Err("No columns were matched by the regex. Check it correctly identifies the appropriate serotype data columns")

    for col in all_matched_cols
        colnm = col.match
        colcap = col.captures

        length(colcap) == 1 ||
            return Try.Err("Only 1 capture group should exist for the column $colnm. Found $(length(colcap)): $colcap.")
        colcap = colcap[1]
        colcap in ["count", "pct"] ||
            return Try.Err("The capture group is not expected. It should be one of [\"count\", \"pct\"], but instead it is $colcap")

        calculated_col = colnm * "_calculated"
        calculated_present = calculated_col in colnames

        # If both count column type has both provided and calculated columns, only keep provided
        if calculated_present && colcap == "count"
            select!(df, Not(calculated_col))
        end

        if calculated_present && colcap == "pct"
            show_warnings && @warn "Using calculated seroprevalence values for column $colnm"
            select!(df, Not(colnm))
            rename!(df, calculated_col => colnm)
        end
    end

    # Columns that only have a calculated count column (and not a provided one) should be renamed to remove "_calculated"
    calc_reg = update_regex(
        reg,
        r"(.*)\$",
        s"(\1)_calculated",
    )

    rename!(
        n -> replace(
            n,
            calc_reg => s"\1"
        ),
        df,
    )
    return Try.Ok(nothing)
end
