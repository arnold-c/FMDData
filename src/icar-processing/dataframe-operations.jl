using DataFrames: DataFrame
using Try

export combine_round_dfs, combine_all_processed_data


"""
    combine_all_processed_data(selection::String = "all"; cols = :union)

Load and combine processed CSV files from the processed directory into a single DataFrame.

# Selection Options
- `"all"`: All processed files in the directory
- `"2019"`: Files matching pattern "*_2019.csv" (state-specific 2019 data)
- `"nadcp"`: Files matching pattern "nadcp_[123].csv" (NADCP rounds 1, 2, 3)
- `"organized_farms"`: Files containing "organized_farms" (farm survey data)

# Arguments
- `selection`: String specifying which files to combine
- `cols`: Column handling strategy for vcat (:union allows different columns)

# Returns
Combined DataFrame with all selected processed data

# Errors
Throws error if selection is invalid or no matching files found

# Examples
```julia
# Combine all data
all_data = combine_all_processed_data("all")

# Only NADCP surveillance data
nadcp_data = combine_all_processed_data("nadcp")
```
"""
function combine_all_processed_data(selection::String = "all"; cols = :union)
    # Get all CSV files from processed directory
    processed_dir = icar_processed_dir()
    all_csv_files = filter(f -> endswith(f, ".csv"), readdir(processed_dir))

    # Filter files based on selection
    csv_files = if selection == "all"
        all_csv_files
    elseif selection == "2019"
        filter(f -> contains(f, "_2019.csv"), all_csv_files)
    elseif selection == "nadcp"
        filter(f -> contains(f, r"nadcp_[123]\.csv"), all_csv_files)
    elseif selection == "organized_farms"
        filter(f -> contains(f, "organized_farms"), all_csv_files)
    else
        error("Invalid selection: $selection. Options are: 'all', '2019', 'nadcp', 'organized_farms'")
    end

    if isempty(csv_files)
        return error("No files found for selection: $selection")
    end

    # Load each file and collect into vector
    dataframes = DataFrame[]
    for filename in csv_files
        df_result = Try.@? load_csv(filename, processed_dir)
        push!(dataframes, df_result)
    end

    return vcat(dataframes...; cols = cols)
end


"""
    combine_round_dfs(dfs::DataFrame...)

Combines multiple DataFrames into a single DataFrame with permissive column handling.
"""
function combine_round_dfs(
        dfs::DataFrame...; cols = :setequal
    )
    return Try.Ok(vcat(dfs..., cols = cols))
end
