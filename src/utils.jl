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

show_warnings = @load_preference("show_warnings", true)

function _log_try_error(res, type::Symbol = :Error; unwrap_ok = true)
    @assert type in [:Error, :Warn, :Info]
    if Try.iserr(res)
        if type == :Error
            show_warnings && @error Try.unwrap_err(res)
            Try.unwrap_err(res)
        elseif type == :Warn
            show_warnings && @warn Try.unwrap_err(res)
            return Try.unwrap_err(res)
        elseif type == :Info
            show_warnings && @info Try.unwrap_err(res)
            return Try.unwrap_err(res)
        end
    end

    if unwrap_ok
        return Try.unwrap(res)
    end
    return res
end

skip_missing_and_nan = Skipper.skip(x -> ismissing(x) || isnan(x))
skip_nothing = Skipper.skip(x -> isnothing(x))

function update_regex(
        original_reg::Regex,
        find_reg::Regex,
        subsitution_str::SubstitutionString
    )
    new_reg = Regex(
        replace(
            original_reg.pattern,
            find_reg => subsitution_str,
        )
    )
    return new_reg
end
