#%%
using DrWatson
@quickactivate "FMDData"

using FMDData
using Try
using DataFrames
using OrderedCollections: OrderedDict
using Logging
using LoggingExtras

#%%
"""
    report_specific_processing_files_dir(args...)

Returns the absolute path to the `report-specific-processing-files` directory within the processed data folder,
which contains individual processing files for specific ICAR reports that are included in the main
processing pipeline.
"""
report_specific_processing_files_dir(args...) = scriptsdir("report-specific-processing-files", args...)

icar_processed_logdir = icar_processed_dir("logfiles")
isdir(icar_processed_logdir) || mkpath(icar_processed_logdir)

#%%
# Wrap in a being block so an error in any section of the script will
# exit execution - including if being run in a REPL with `include()`,
# which would otherwise just move to the next code section and continue
begin
    # Process NADCP rounds with cumulative data inference
    processing_files = [
        ("process-nadcp-2.jl", "NADCP-2 (2021/2022 cumulative data with inference)"),
        ("process-nadcp-1.jl", "NADCP-1 (2020/2021 cumulative data with inference)"),
        ("process-2021-organized-farms.jl", "2021 Organized farms"),
        ("process-2022-nadcp-3.jl", "2022 NADCP-3"),
        ("process-2022-organized-farms.jl", "2022 Organized farms"),
        ("process-2019-state-files.jl", "2019 State files (all 23 states)"),
    ]

    println("="^60)
    println("Processing cleaned data")
    println("="^60)

    for (i, (filename, description)) in pairs(processing_files)
        println("[$i/$(length(processing_files))] Processing $description...")
        include(report_specific_processing_files_dir(filename))
        println("✓ Completed $description")
        println()
    end

    println("="^60)
    println("All processing complete!")
    println("="^60)

    # Combine processed data with different selection options
    println("Creating combined datasets...")

    # All data
    println("Combining all processed data...")
    all_data = combine_all_processed_data("all")
    println("✓ Combined $(nrow(all_data)) rows from all files")
    Try.@? write_csv("all_combined_icar_data.csv", icar_processed_dir(), all_data)

    # 2019 state files only
    println("Combining 2019 state files...")
    data_2019 = combine_all_processed_data("2019")
    println("✓ Combined $(nrow(data_2019)) rows from 2019 state files")
    Try.@? write_csv("combined_2019_states.csv", icar_processed_dir(), data_2019)

    # NADCP files only
    println("Combining NADCP files...")
    nadcp_data = combine_all_processed_data("nadcp")
    println("✓ Combined $(nrow(nadcp_data)) rows from NADCP files")
    Try.@? write_csv("combined_nadcp_data.csv", icar_processed_dir(), nadcp_data)

    # Organized farms only
    println("Combining organized farms files...")
    farms_data = combine_all_processed_data("organized_farms")
    println("✓ Combined $(nrow(farms_data)) rows from organized farms files")
    Try.@? write_csv("combined_organized_farms.csv", icar_processed_dir(), farms_data)

    println("✓ All combined datasets exported successfully")
    println("="^60)
end
