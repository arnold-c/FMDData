using DataFrames: DataFrame, transform, ByRow
using Try: Try

export correct_all_state_names,
    check_missing_states,
    check_duplicated_states

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
        states_dict::Dict = states_dict
    )

    corrected_df = transform(
        df,
        column => ByRow(s -> Try.and_then(String, correct_state_name(s, states_dict)));
        renamecols = false
    )

    name_errors_idxs = isa.(corrected_df[!, column], Try.Err)
    if sum(name_errors_idxs) > 0
        name_errors = convert(Vector{Try.Err}, corrected_df[name_errors_idxs, column])
        return Try.Err(_combine_error_messages(name_errors))
    end

    return Try.Ok(corrected_df)
end

"""
	correct_state_name(
        input_name::String,
        states_dict::Dict = FMDData.states_dict
    )

Check if a state name is correctly spelled, or previously characterized and matched with a correct name. Returns the correct name if possible, or errors.
"""
function correct_state_name(
        input_name::S,
        states_dict::Dict = states_dict
    ) where {S <: AbstractString}
    possible_state_values = values(states_dict)

    if in(input_name, possible_state_values) || lowercase(input_name) == "total"
        return Try.Ok(input_name)
    end

    possible_state_keys = keys(states_dict)
    in(input_name, possible_state_keys) ||
        return Try.Err("State name `$input_name` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling.")

    return Try.Ok(states_dict[input_name])
end


"""
    check_missing_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )

Check if the states column of the data contains missing values
"""
function check_missing_states(
        df::DataFrame,
        column::Symbol = :states_ut,
    )
    nmissing = sum(ismissing.(df[!, column]))
    nmissing == 0 || return Try.Err("There are $nmissing values in the $column column that are of type `Missing`")
    return Try.Ok(nothing)
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
    states = String.(filter(!ismissing, df[!, column]))
    nstates = length(states)
    unique_states = unique(states)
    state_counts = _calculate_string_occurences(states, unique_states)

    nstates == length(unique_states) || return Try.Err("The dataframe has $nstates state values, but only $(length(unique_states)) unique state values. $(String.(keys(filter(c -> values(c) != 1, state_counts)))) were duplicated")

    return Try.Ok(nothing)
end
