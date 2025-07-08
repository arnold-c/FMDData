# Additional Data Processing

This document outlines the additional data processing steps applied after the initial cleaning phase. This process is primarily for handling cumulative datasets, such as the NADCP reports, where data for a single year needs to be inferred. The process is orchestrated by the `scripts/icar-additional-processing.jl` script.

### Additional Processing Workflow

The `scripts/icar-additional-processing.jl` script executes a series of steps to load cleaned data, infer yearly values from cumulative reports, add essential metadata, and combine datasets for further analysis.

**Step 1: Loading Cleaned Data**

*   The script begins by loading the relevant cleaned CSV files from the `data/icar-seroprevalence/cleaned/` directory. For a given NADCP round, this typically includes data for consecutive years. For example, for "NADCP 2", it loads the cleaned reports for 2021 and 2022.

**Step 2: Inferring Later Year Values**

*   For reports that provide cumulative data (e.g., the 2022 NADCP 2 report contains cumulative data for 2021 and 2022), the script infers the values for the most recent year.
*   This is achieved using the `infer_later_year_values` function from `src/icar-processing/icar-processing-functions.jl`.
*   The function works by subtracting the counts from the initial year's dataset (e.g., 2021) from the cumulative dataset (e.g., 2022).
*   After subtraction, it performs a series of corrections and recalculations:
    *   It recalculates the "Total" row for all count and percentage columns based on the new single-year values.
    *   It recalculates the seroprevalence percentages for each state.

**Step 3: Adding Metadata**

*   To enrich the data and provide essential context for analysis, several metadata columns are added to the dataframes using the `add_all_metadata!` function.
*   The following metadata are added:
    *   `:sample_year`: The year(s) the samples were collected.
    *   `:report_year`: The year the report was published.
    *   `:round_name`: The name of the vaccination or testing round (e.g., "NADCP 2").
    *   `:test_type`: The serological test used (e.g., "SPCE").
    *   `:test_threshold`: The threshold for a positive result.
*   This metadata is applied to the original cleaned dataframes, the newly inferred single-year dataframe, and the cumulative dataframes.

**Step 4: Combining and Saving**

*   The `combine_round_dfs` function is used to vertically concatenate the individual dataframes (e.g., the inferred 2022 data, the 2021 data, and the cumulative 2021/2022 data) into a single, comprehensive DataFrame for that round.
*   The final processed DataFrames (both individual years and the combined set) are saved as new CSV files in the `data/icar-seroprevalence/processed/` directory.

### Summary

The additional processing workflow serves two main purposes:

1.  **Derives Single-Year Data**: It extracts single-year statistics from cumulative reports, which is crucial for longitudinal analysis.
2.  **Enriches Data**: It adds critical metadata to the datasets, making them self-contained and analysis-ready.

This ensures that the final processed data is accurate, consistent, and ready for use in modeling and visualization.
