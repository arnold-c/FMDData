# Processing Functions

The processing stage takes cleaned data and adds metadata, infers missing values, and combines datasets. This stage follows the cleaning pipeline and prepares data for analysis by enriching it with contextual information and combining multiple datasets into unified structures.

## Data Inference

Functions for inferring missing or incomplete data values based on patterns in the dataset:

```@docs
infer_later_year_values
```

## DataFrame Operations

Functions for combining and manipulating cleaned datasets from different sources or time periods:

```@docs
combine_round_dfs
combine_all_processed_data
```

These functions handle the combination of multiple cleaned datasets, typically used when processing data from different testing rounds (NADCP 1, NADCP 2, NADCP 3) or organized farm surveys across different time periods.

## Metadata Operations

### Main Metadata Function

The primary function for adding comprehensive metadata to processed datasets:

```@docs
add_all_metadata!
```

### Specific Metadata Functions

Individual functions for adding specific types of metadata. These are typically called through `add_all_metadata!` but can be used individually for specific metadata requirements:

```@docs
add_test_threshold!
add_test_type!
add_round_name!
add_report_year!
add_sample_year!
add_metadata_col!
```

The metadata functions enrich the cleaned data with contextual information including:
- **Test thresholds**: Diagnostic test cutoff values used for seroprevalence determination
- **Test types**: The specific diagnostic assay used (e.g., ELISA, VNT)
- **Round names**: Testing campaign identifiers (NADCP 1, NADCP 2, etc.)
- **Report year**: The year the ICAR annual report was published
- **Sample year**: The year when samples were collected (may differ from report year)
- **Custom metadata**: Additional contextual information as needed
