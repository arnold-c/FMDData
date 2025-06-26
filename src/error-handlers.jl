using Try: Try
using Preferences: @load_preference

show_warnings = @load_preference("show_warnings", true)

"""
    _log_try_error(res, type::Symbol = :Error; unwrap_ok = true)

Internal function. Checks a `Try` result. If it's an `Err`, it logs the error message and returns the unwrapped error. If it's an `Ok`, it returns the unwrapped value by default.

This function helps manage control flow by logging non-critical errors without halting execution, while still allowing critical errors to be propagated.

# Arguments
- `res`: The `Try.Ok` or `Try.Err` object to check.
- `type::Symbol`: The logging level to use if `res` is an `Err`. Can be `:Error`, `:Warn`, or `:Info`. Defaults to `:Error`.
- `unwrap_ok::Bool`: If `true`, returns the unwrapped value of an `Ok` result. If `false`, returns the `Try.Ok` object itself. Defaults to `true`.
"""
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

    if unwrap_ok && Try.isok(res)
        return Try.unwrap(res)
    end
    return res
end


"""
    _combine_error_messages(arr_of_errs::AbstractVector{T}; filter_ok = false) where {T <: Try.InternalPrelude.AbstractResult}

Internal function. Combines error messages from a vector of `Try` results into a single string.

This is useful for aggregating multiple errors into a single, more informative error message.

# Arguments
- `arr_of_errs`: A vector of `Try.Ok` or `Try.Err` objects.
- `filter_ok`: If `true`, `Try.Ok` results are filtered out before combining messages. Defaults to `false`.
"""
function _combine_error_messages(
        arr_of_errs::AbstractVector{T};
        filter_ok = false
    ) where {T <: Try.InternalPrelude.AbstractResult}
    if filter_ok
        filter!(!Try.isok, arr_of_errs)
    end
    return String(
        strip(
            mapreduce(
                _unwrap_err_or_empty_str,
                (acc, next_val) -> acc * " " * next_val,
                arr_of_errs
            )
        )
    )
end

"""
    _unwrap_err_or_empty_str(res)

Internal funciton. Unwraps a `Try.Err` to get its error message, or returns an empty string for a `Try.Ok`.

This function is a helper for `_combine_error_messages`, ensuring that only error messages are included in the final combined string.
"""
function _unwrap_err_or_empty_str(res::Union{Try.Ok{<:T}, Try.Err{<:E}}) where {T <: AbstractString, E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return Try.unwrap(res)
end

function _unwrap_err_or_empty_str(res::Union{Try.Ok{Nothing}, Try.Err{<:E}}) where {E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return ""
end
