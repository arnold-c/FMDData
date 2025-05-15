using DrWatson: DrWatson
using Try: Try
using Skipper: Skipper
export input_dir,
    icar_inputs_dir,
    icar_outputs_dir,
    icar_cleaned_dir,
    icar_processed_dir,
    skip_missing_and_nan,
    update_regex

input_dir(args...) = DrWatson.projectdir("inputs", args...)
icar_inputs_dir(args...) = input_dir("ICAR-Reports", "extracted-seroprevalence-tables", args...)
icar_outputs_dir(args...) = DrWatson.datadir("icar-seroprevalence", args...)
icar_cleaned_dir(args...) = icar_outputs_dir("cleaned", args...)
icar_processed_dir(args...) = icar_outputs_dir("processed", args...)

function _log_try_error(res, type::Symbol = :Error; unwrap_ok = true)
    @assert type in [:Error, :Warn, :Info]
    if Try.iserr(res)
        if type == :Error
            @error Try.unwrap_err(res)
            Try.unwrap_err(res)
        elseif type == :Warn
            return @warn Try.unwrap_err(res)
        elseif type == :Info
            return @info Try.unwrap_err(res)
        end
    end

    if unwrap_ok
        return Try.unwrap(res)
    end
    return res
end

skip_missing_and_nan = Skipper.skip(x -> ismissing(x) || isnan(x))

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
