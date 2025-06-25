using DrWatson: DrWatson
using Try: Try
using Skipper: Skipper
using Preferences: @load_preference

export input_dir,
    icar_inputs_dir,
    icar_outputs_dir,
    icar_cleaned_dir,
    icar_processed_dir,
    skip_missing_and_nan,
    skip_nothing,
    update_regex

input_dir(args...) = DrWatson.projectdir("inputs", args...)
icar_inputs_dir(args...) = input_dir("ICAR-Reports", "extracted-seroprevalence-tables", args...)
icar_outputs_dir(args...) = DrWatson.datadir("icar-seroprevalence", args...)
icar_cleaned_dir(args...) = icar_outputs_dir("cleaned", args...)
icar_processed_dir(args...) = icar_outputs_dir("processed", args...)

"""
	skip_missing_and_nan

Convenience function to skip missing and/or NaN values in a iterator.

Returns another iterator.
"""
skip_missing_and_nan = Skipper.skip(x -> ismissing(x) || isnan(x))


"""
	skip_nothing

Convenience function to skip nothing values in an iterator

Returns another iterator.
"""
skip_nothing = Skipper.skip(x -> isnothing(x))

"""
    update_regex(
        original_reg::Regex,
        find_reg::Regex,
        substitution_str::SubstitutionString
    )

Update a Regex string using regex and a substitution string.
"""
function update_regex(
        original_reg::Regex,
        find_reg::Regex,
        substitution_str::SubstitutionString
    )
    new_reg = Regex(
        replace(
            original_reg.pattern,
            find_reg => substitution_str,
        )
    )
    return new_reg
end

"""
    _calculate_string_occurences(
        vals::Vector{S},
        unique_vals::Vector{S} = unique(vals)
    ) where {S <: AbstractString}

Internal function to calculate how many times each unique string value occurs in a vector of strings
"""
function _calculate_string_occurences(
        vals::Vector{S},
        unique_vals::Vector{S} = unique(vals)
    ) where {S <: AbstractString}
    return NamedTuple{tuple(Symbol.(unique_vals)...)}(
        map(
            i -> sum(i .== vals),
            unique_vals,
        )
    )
end
