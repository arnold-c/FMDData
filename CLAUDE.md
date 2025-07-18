# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FMDData.jl is a Julia package for cleaning and processing Foot-and-Mouth Disease (FMD) seroprevalence data from Indian Council of Agricultural Research (ICAR) annual reports. The package processes raw CSV tables extracted from PDF reports into standardized, cleaned datasets with proper metadata.

## Development Commands

### Package Management
- **Activate project**: `julia --project=.`
- **Install dependencies**: `julia --project=. -e "using Pkg; Pkg.instantiate()"`
- **Run tests**: `julia --project=. -e "using Pkg; Pkg.test()"`

### Data Processing Scripts
- **Run cleaning pipeline**: `include("./scripts/icar-cleaning.jl")`
- **Run additional processing**: `include("./scripts/icar-additional-processing.jl")`

Note: The cleaning script currently terminates early due to irredeemable errors in some raw files. To work around this, send individual lines of code to a running Julia session.

### Documentation
- **Build docs**: `julia --project=docs docs/make.jl`
- **Docs are deployed to**: https://fmddata.callumarnold.com

### Testing
- **Run all tests**: `julia --project=. test/runtests.jl`
- **Individual test files**: Tests are organized by functionality in `test/icar-cleaning/`
- **Integration tests**: Available in `test/icar-cleaning/integration-test.jl`

## Architecture Overview

### Data Pipeline
The package follows a two-stage processing pipeline:

1. **Cleaning Stage** (`src/icar-cleaning/`):
   - Input: Raw CSV files in `inputs/ICAR-Reports/extracted-seroprevalence-tables/`
   - Output: Cleaned CSV files in `data/icar-seroprevalence/cleaned/`
   - Functions: Data quality checks, column standardization, state name validation

2. **Processing Stage** (`src/icar-processing/`):
   - Input: Cleaned CSV files
   - Output: Processed CSV files in `data/icar-seroprevalence/processed/`
   - Functions: Metadata addition, sample year inference, data aggregation

### Key Module Structure
- **Main module**: `src/FMDData.jl` - Includes all submodules and handles precompilation
- **Constants**: `src/consts.jl` - Defines file paths and data constants
- **Utilities**: `src/utils.jl` - Common helper functions
- **Error handling**: `src/error-handlers.jl` - Standardized error management with Try.jl

### ICAR Cleaning Functions (`src/icar-cleaning/`)
Core cleaning functions are organized by functionality:
- **State validation**: `state-checks.jl`, `state-keys.jl` - Validates against known Indian states
- **Column management**: `clean-column-names.jl`, `column-name-checks.jl`
- **Data validation**: `check-calculated-values.jl`, `check-seroprevalence-values.jl`
- **Calculations**: `calculate-state-counts.jl`, `calculate-state-seroprevalence.jl`
- **Workflow**: `wrapper-functions.jl` - Main entry point `all_cleaning_steps()`

### Error Handling Strategy
- **WARNINGS**: Recoverable errors (logged but processing continues, e.g., using calculated values when provided values appear incorrect)
- **ERRORS**: Fatal errors that stop processing (e.g., unrecognized state names)
- All functions use `Try.jl` for consistent error handling via `_log_try_error()`

### Testing Strategy
- Unit tests mirror the `src/` structure in `test/`
- Test data provided in `test/test-data.csv`
- Integration tests validate the full cleaning pipeline
- JET.jl and Aqua.jl for static analysis (currently commented out in test suite)

### Data Flow
```
inputs/ICAR-Reports/extracted-seroprevalence-tables/*.csv
    ↓ (scripts/icar-cleaning.jl)
data/icar-seroprevalence/cleaned/*.csv + logfiles/
    ↓ (scripts/icar-additional-processing.jl)
data/icar-seroprevalence/processed/*.csv + logfiles/
```

## Working with State Data
The package processes seroprevalence data for Indian states. Key considerations:
- State names must match those defined in `src/icar-cleaning/state-keys.jl`
- Some files contain state-level data, others contain farm-level data
- Data includes different testing rounds (NADCP 1, NADCP 2, NADCP 3) and organized farms
- Files from different years (2019-2022) have varying structures

## Dependencies
Key packages used:
- **DataFrames.jl**: Primary data structure
- **CSV.jl**: File I/O
- **DrWatson.jl**: Project management and paths
- **Try.jl/TryExperimental.jl**: Error handling
- **LoggingExtras.jl**: Enhanced logging for data processing pipeline