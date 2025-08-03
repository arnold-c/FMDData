# Common Functions

This module provides core utilities and constants used throughout the FMDData.jl package for processing Foot-and-Mouth Disease seroprevalence data.

## Main Module

The main FMDData module that orchestrates all data processing functionality:

```@docs
FMDData
```

## Constants

Package-wide constants that define standard values and configurations:

```@docs
default_allowed_serotypes
```

## Utility Functions

Core utility functions that support data processing operations across the package.

### Directory Management

Functions for managing file paths and directory structures used in the data processing pipeline:

```@docs
input_dir
icar_inputs_dir
icar_outputs_dir
icar_cleaned_dir
icar_processed_dir
```

### Data Processing Utilities

Helper functions for common data manipulation tasks and filtering operations:

```@docs
skip_missing_and_nan
skip_nothing
update_regex
```


