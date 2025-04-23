using CSV
using DataFrames: DataFrame, select, subset, filter, rename, transform!, ByRow

export load_csv,
    clean_colnames,
    totals_check,
    has_totals_row,
    check_duplicated_states,
    check_aggregated_pre_post_counts,
    rename_aggregated_pre_post_counts,
    check_state_names,
    correct_state_name,
    correct_all_state_names

"""
    load_csv(
        filename::T1,
        dir::T1,
        output_format = DataFrame
    ) where {T1 <: AbstractString}

A helper function to check if a csv input file and directory exists, and if so, load (as a DataFrame by default).
"""
function load_csv(
        filename::T1,
        dir::T1,
        output_format = DataFrame
    ) where {T1 <: AbstractString}
    isdir(dir) || error("$dir is not a valid directory")
    contains(filename, r".*\.csv$")    || error("$filename is not a csv file")

    dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(dir))
    in(filename, dir_files) || error("$filename is not within the directory $dir")

    return CSV.read(
        joinpath(dir, filename),
        output_format
    )
end

"""
    clean_colnames(df::DataFrame)

Replace spaces and / with underscores
"""
function clean_colnames(df::DataFrame)
    return rename(
        t -> lowercase(replace(t, "/" => "_", " " => "_")),
        df
    )
end

"""
    check_duplicated_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )

Check if there are duplicated states in the data
"""
function check_duplicated_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )
    return @assert length(df[!, column]) == length(unique(df[!, column]))
end

"""
    check_aggregated_pre_post_counts(
        df::DataFrame,
    )

Check if data contains aggregated counts of pre and post vaccinated individuals
"""
function check_aggregated_pre_post_counts(df::DataFrame, columns = ["pre_(n)", "post_(n)"])
    return @assert sum(map(c -> in(c, names(df)), columns)) == length(columns)
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
        original_regex::Regex = r"^(p.*)_\(n\)",
        substitution_string::SubstitutionString = s"serotype_all_(n)_\1"
    )
    return rename(
        s -> replace(s, original_regex => substitution_string),
        df
    )
end


"""
    has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )

Check if the table has a totals row.

`df` should have, at the very least, cleaned column names using the `clean_colnames()` function.
"""
function has_totals_row(
        df::DataFrame,
        column::Symbol = :states_ut,
        possible_keys = ["total", "totals"]
    )
    return length(filter(s -> in(s, possible_keys), lowercase.(df[!, column]))) > 0
end

function contains_seroprev_results(df, serotypes = ["all", "o", "a", "asia1"])

end

function contains_count_results(df, serotypes = ["all", "o", "a", "asia1"])

end

"""
    totals_check(df::DataFrame, totals_key = "total")

TBW
"""
function totals_check(df::DataFrame, totals_key = "total")
    return nothing
end

"""
    correct_all_state_names(
        df::DataFrame,
        column::Symbol = :states_ut,
        states_dict::Dict = FMDData.states_dict
    )

Correct all state name values in the data
"""
function correct_all_state_names(
        df::DataFrame,
        column::Symbol = :states_ut,
        states_dict::Dict = FMDData.states_dict
    )
    df_state_keys = df[!, column]

    df2 = copy(df)

    transform!(
        df2,
        column => ByRow(s -> correct_state_name(s, states_dict))
    )

    return df2
end

"""
	correct_state_name(
        input_name::String,
        states_dict::Dict = FMDData.states_dict
    )

Check if a state name is correctly spelled, or previously characterized and matched with a correct name. Returns the correct name if possible, or errors.
"""
function correct_state_name(
        input_name::String,
        states_dict::Dict = FMDData.states_dict
    )
    possible_state_values = values(states_dict)

    if in(input_name, possible_state_values)
        return input_name
    end

    possible_state_keys = keys(states_dict)
    in(input_name, possible_state_keys) ||
        error("State name `$input_name` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling")

    return states_dict[input_name]
end
