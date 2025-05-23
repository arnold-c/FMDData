using Try: Try
using Preferences: @load_preference

show_warnings = @load_preference("show_warnings", true)

"""
    _log_try_error(res, type::Symbol = :Error; unwrap_ok = true)

Internal function that checks if the value received is a Try.Err() object, and if so, logs the error with the level provided by the `type` argument. Useful to choose when to stop for an error, and when to log and continue.
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
    _combine_error_messages(arr_of_errs::AbstractVector{T}) where {T <: Try.InternalPrelude.AbstractResult}

Internal function that accepts a vector of `Try` results e.g., `Ok()` and `Err()`, and concatenates them to be passed up the call stack.
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

Internal function to check if result is an error and if so, return the unwrapped (error message) value. If the result is an Ok() result, return an empty string that will be used to during concatenation of error messages.
"""
function _unwrap_err_or_empty_str(res::Union{Try.Ok{<:T}, Try.Err{<:E}}) where {T <: AbstractString, E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return Try.unwrap(res)
end

function _unwrap_err_or_empty_str(res::Union{Try.Ok{Nothing}, Try.Err{<:E}}) where {E}
    Try.iserr(res) && return Try.unwrap_err(res)
    return ""
end
