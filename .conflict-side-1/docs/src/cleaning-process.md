# Data Cleaning Process

This document outlines the data cleaning process for the FMDData project. The process is orchestrated by the `scripts/icar-cleaning.jl` script, which uses two distinct cleaning pipelines to handle variations in the source data formats.

### Cleaning Pipelines

The `scripts/icar-cleaning.jl` script segregates files into two main cleaning workflows:

1.  **`all_cleaning_steps`**: The standard pipeline for NADCP and post-2019 reports.
2.  **`all_2019_cleaning_steps`**: A specialized pipeline for the 2019 annual report data, which has a different structure.

---

### Main Workflow: `all_cleaning_steps`

This function, defined in `src/icar-cleaning/wrapper-functions.jl`, is a multi-stage process for loading, validating, calculating, and saving the data.

**Step 1: Setup and Logging**

*   A `logfiles` directory and a log file for the specific dataset are created.
*   Each step is wrapped in a `_log_try_error` block to capture warnings and errors without halting execution.

**Step 2: Initial Loading and Sanitization**

1.  **`load_csv`**: Loads the raw CSV file from `inputs/ICAR-Reports/extracted-seroprevalence-tables/` into a DataFrame.
2.  **`clean_colnames`**: Standardizes column names by converting to `snake_case`, removing special characters, and trimming whitespace.
3.  **`rename_aggregated_pre_post_counts`**: Renames columns for pre- and post-vaccination counts for clarity.
4.  **`correct_all_state_names`**: Standardizes state names using a predefined mapping in `src/icar-cleaning/state-keys.jl`.

**Step 3: Data Validation**

A series of validation checks are performed to ensure data integrity.

*   **Structural Checks**:
    *   `check_duplicated_column_names`: Ensures no duplicate column names exist after cleaning.
    *   `check_duplicated_states`: Verifies that each state appears only once.
*   **Content and Completeness Checks**:
    *   `check_missing_states`: Confirms that all expected states are present.
    *   `check_allowed_serotypes`: Validates that serotype columns match a predefined list.
    *   `check_seroprevalence_as_pct`: Ensures seroprevalence values are percentages.
    *   `check_aggregated_pre_post_counts_exist` and `check_pre_post_exists`: Confirms the presence of essential columns for vaccination counts.

**Step 4: Totals Calculation and Verification**

1.  **`has_totals_row`**: Checks for the existence of a "Total" row.
2.  **`calculate_all_totals`**: Independently calculates totals for all numeric columns.
3.  **`all_totals_check`**: Compares calculated totals with existing totals. If they do not match, a warning is logged, and the calculated totals are used. If no totals row exists, the calculated one is added.

**Step 5: Core Calculations and Finalization**

1.  **`calculate_state_counts` & `calculate_state_seroprevalence`**: Calculates state-level counts and seroprevalence.
2.  **`check_calculated_values_match_existing`**: Compares these calculations with the original values and logs any discrepancies.
3.  **`select_calculated_cols!` & `select_calculated_totals!`**: Replaces the original columns with the calculated ones.
4.  **`sort_columns!` & `sort_states!`**: Sorts columns and rows for consistency.

**Step 6: Output**

The cleaned DataFrame is saved as a new CSV file in the `data/icar-seroprevalence/cleaned/` directory.

---

### Specialized Workflow: `all_2019_cleaning_steps`

The 2019 data requires a different cleaning process due to its unique structure.

1.  **No Totals Row**: The 2019 tables do not contain a "Total" row. The function will log an error if one is found.

2.  **Duplicate States**: The 2019 data may contain multiple rows for the same state. The `check_duplicated_states` validation step is skipped in this pipeline.

3.  **Conditional Calculations**: The core calculations (`calculate_state_counts`, `calculate_state_seroprevalence`) are only performed if aggregated count columns are present.

### Summary

The FMDData project uses two distinct cleaning pipelines to handle different data formats:

*   **`all_cleaning_steps`**: A comprehensive pipeline for structured tables with a single row per state and a totals row.
*   **`all_2019_cleaning_steps`**: A flexible pipeline for the 2019 report tables, which lack totals and may have multiple entries per state.

This approach allows for robust and accurate cleaning of various data formats.
