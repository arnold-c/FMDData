"""
    generate_autodocs_pages_list(relative_source_subdir::String)

Print a list of all files in the "src" directory of the associated package. Useful when manually adding pages to be indexed when building docs.
"""
function generate_autodocs_pages_list(relative_source_subdir::String = "")
    # Assumes this script (make.jl) is in docs/
    # and the main package module (e.g., MyPackage.jl) is in ../src/
    base_src_path = joinpath(@__DIR__, "..", "src") # Path to the project's src/ directory
    base_src_path_length = length(base_src_path) + 2
    api_src_path = joinpath(base_src_path, relative_source_subdir)

    if !isdir(api_src_path)
        @warn "Source subdirectory for @autodocs Pages not found: $api_src_path"
        return "[]"
    end

    jl_files = mapreduce(vcat, walkdir(api_src_path)) do (root, dirs, files)
        mapreduce(
            f -> joinpath(root, f)[base_src_path_length:end],
            vcat,
            filter(f -> endswith(f, ".jl"), files)
        )
    end

    # Paths for @autodocs Pages are relative to the package's root source directory (e.g., src/)
    # So, if relative_source_subdir is "api", paths will be "api/file1.jl"
    paths_for_autodocs = [joinpath(relative_source_subdir, f) for f in jl_files]

    if isempty(paths_for_autodocs)
        return "[]"
    end

    println("Pages = [")
    for p in jl_files
        println("\"$p\",")
    end
    println("]")
    return nothing
end
