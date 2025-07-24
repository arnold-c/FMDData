using DataFrames: DataFrame
using Try

export combine_round_dfs, combine_all_processed_data


"""
    combine_all_processed_data(selection::String = "all")

Loads processed CSV files from the processed directory and combines them into a single DataFrame.
Selection options:
- "all": All processed files
- "2019": Only 2019 state files
- "nadcp": Only NADCP files (NADCP-1, NADCP-2, NADCP-3)
- "organized_farms": Only organized farms files

Returns a DataFrame containing the selected processed ICAR seroprevalence data.
"""
function combine_all_processed_data(selection::String = "all")
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

    return vcat(dataframes...; cols = :union)
end


"""
    combine_round_dfs(dfs::DataFrame...)

Combines multiple DataFrames into a single DataFrame with permissive column handling.
"""
function combine_round_dfs(
        dfs::DataFrame...
    )
    return Try.Ok(vcat(dfs...))
end
