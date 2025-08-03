# Additional Data Processing

This document outlines the additional data processing steps applied after the initial cleaning phase. The process handles various data types including cumulative datasets (NADCP reports), organized farm surveys, and state-specific files. The process is orchestrated by the `scripts/icar-additional-processing.jl` script, which coordinates multiple report-specific processing files.

### Additional Processing Workflow

The `scripts/icar-additional-processing.jl` script executes a coordinated series of processing steps through individual report-specific files, then combines the results into unified datasets for analysis.

**Step 1: Report-Specific Processing**

The main script coordinates the execution of individual processing files located in `scripts/report-specific-processing-files/`. Each file handles a specific dataset type:

*   **`process-nadcp-2.jl`**: Processes NADCP-2 data (2021/2022) with cumulative data inference
*   **`process-nadcp-1.jl`**: Processes NADCP-1 data (2020/2021) with cumulative data inference  
*   **`process-2021-organized-farms.jl`**: Processes 2021 organized farm survey data
*   **`process-2022-nadcp-3.jl`**: Processes 2022 NADCP-3 data
*   **`process-2022-organized-farms.jl`**: Processes 2022 organized farm survey data
*   **`process-2019-state-files.jl`**: Processes all 23 state files from the 2019 report

**Step 2: Individual Processing Operations**

Each report-specific file performs the following operations as needed:

*   **Loading Cleaned Data**: Loads relevant cleaned CSV files from `data/icar-seroprevalence/cleaned/`
*   **Inferring Later Year Values**: For cumulative datasets (NADCP reports), uses `infer_later_year_values` to extract single-year data by subtracting previous year counts from cumulative totals
*   **Adding Metadata**: Applies `add_all_metadata!` to enrich datasets with contextual information:
    *   `:sample_year`: Year(s) when samples were collected
    *   `:report_year`: Year the report was published
    *   `:round_name`: Testing/vaccination round identifier (e.g., "NADCP 2")
    *   `:test_type`: Serological test used (e.g., "SPCE")
    *   `:test_threshold`: Threshold for positive results
*   **Combining Round Data**: Uses `combine_round_dfs` to vertically concatenate related dataframes
*   **Saving Results**: Exports processed data to `data/icar-seroprevalence/processed/`

**Step 3: Dataset Combination**

After all individual processing is complete, the script creates unified datasets using `combine_all_processed_data` with different selection criteria:

*   **All Data**: Combines all processed files into a comprehensive dataset (`all_combined_icar_data.csv`)
*   **2019 State Files**: Combines only 2019 state-specific data (`combined_2019_states.csv`)
*   **NADCP Data**: Combines all NADCP round data (`combined_nadcp_data.csv`)
*   **Organized Farms**: Combines all organized farm survey data (`combined_organized_farms.csv`)

### Summary

The additional processing workflow serves multiple key purposes:

1.  **Modular Processing**: Uses report-specific processing files to handle different data types and structures appropriately
2.  **Derives Single-Year Data**: Extracts single-year statistics from cumulative reports (NADCP data), crucial for longitudinal analysis
3.  **Enriches Data**: Adds critical metadata to all datasets, making them self-contained and analysis-ready
4.  **Creates Unified Datasets**: Combines processed data into logical groupings for different analysis needs
5.  **Maintains Data Provenance**: Preserves information about data sources and processing steps through metadata

This modular approach ensures that the final processed data is accurate, consistent, and organized for various analytical purposes including modeling, visualization, and comparative studies across different testing rounds and geographical regions.
