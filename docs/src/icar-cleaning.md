# Cleaning Functions

The cleaning pipeline processes raw CSV tables extracted from ICAR annual reports into standardized, validated datasets. The pipeline performs data quality checks, column standardization, state name validation, and calculations verification.

## Main Wrapper Functions

The cleaning pipeline is orchestrated through a wrapper function that combines multiple cleaning steps in the correct sequence:

```@docs
all_cleaning_steps
```

## State Reference Data

The package validates state names against a comprehensive dictionary of Indian states and union territories:

```@docs
FMDData.states_dict
FMDData.state_code_dict
```

## File Management

Core I/O operations for loading and saving CSV data with proper error handling:

```@docs
load_csv
write_csv
```

## Column Name Processing

Functions for standardizing column names and detecting structural issues:

```@docs
clean_colnames
rename_aggregated_pre_post_counts
check_duplicated_column_names
check_duplicated_columns
```

## State Data Processing

Functions for validating and standardizing state names using the reference dictionary:

```@docs
correct_all_state_names
check_missing_states
check_duplicated_states
add_state_code!
```

## Data Calculations

Functions for calculating derived values from raw count data:

### Count Calculations
```@docs
calculate_state_counts
```

### Seroprevalence Calculations
```@docs
calculate_state_seroprevalence
```

## Data Validation

Comprehensive validation functions to ensure data quality and consistency:

### Value Checks
```@docs
check_calculated_values_match_existing
check_seroprevalence_as_pct
```

### Serotype Validation
```@docs
check_allowed_serotypes
```

### Pre/Post Vaccination Checks
```@docs
check_pre_post_exists
check_aggregated_pre_post_counts_exist
```

## Totals Processing

Functions for handling and validating totals rows in the datasets:

```@docs
has_totals_row
all_totals_check
calculate_all_totals
totals_check
select_calculated_totals!
```

## Column Selection and Sorting

Final processing steps to organize and standardize the cleaned data structure:

```@docs
select_calculated_cols!
sort_columns!
sort_states!
```
