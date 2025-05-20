using CSV: read, write
using DataFrames: DataFrame
using Try: Try

export load_csv,
    write_csv

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
    isdir(dir) || return Err("$dir is not a valid directory")
    contains(filename, r".*\.csv$") || return Err("$filename is not a csv file")

    dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(dir))
    in(filename, dir_files) || return Err("$filename is not within the directory $dir")

    return Try.Ok(
        read(
            joinpath(dir, filename),
            output_format
        )
    )
end


"""
    write_csv(
        filename::T1,
        dir::T1,
        data::DataFrame
    ) where {T1 <: AbstractString}

A helper function to check if the specified name and directory exist and are valid, and if so, write the CSV to disk.
"""
function write_csv(
        filename::T1,
        dir::T1,
        data::DataFrame
    ) where {T1 <: AbstractString}
    isdir(dir) || mkpath(dir)
    contains(filename, r".*\.csv$") || return Err("$filename is not a csv file")

    write(
        joinpath(dir, filename),
        data
    )
    return Try.Ok(nothing)
end
