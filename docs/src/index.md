# FMDData

This project contains the code and cleaned FMD data sets for India's FMD disease elimination project.

## Structure

```bash
.
├── data/                                    # Cleaned and processed datasets (67 CSV files)
│   ├── diva/                               # DIVA database cleaned data
│   └── icar-seroprevalence/               # ICAR seroprevalence data
│       ├── cleaned/                        # Quality-checked, standardized data (28 files)
│       │   └── logfiles/                   # Cleaning process logs
│       └── processed/                      # Metadata-enriched, analysis-ready data (39 files)
│           └── logfiles/                   # Processing logs
├── docs/                                   # Documentation source and build files
│   └── src/                               # Documentation markdown files
├── inputs/                                 # Raw input data files
│   ├── diva-data.csv                      # Raw DIVA database export
│   └── ICAR-Reports/                      # ICAR annual report data
│       ├── extracted-seroprevalence-tables/ # CSV tables extracted from PDFs (31 files)
│       ├── pdfs/                          # Original ICAR annual report PDFs (8 files)
│       └── README.md                      # ICAR data documentation
├── scripts/                               # Data processing orchestration scripts
│   ├── diva-cleaning.jl                  # DIVA data cleaning script
│   ├── icar-cleaning.jl                  # ICAR data cleaning orchestrator
│   ├── icar-additional-processing.jl     # ICAR data processing orchestrator
│   └── report-specific-processing-files/ # Individual processing scripts (7 files)
│       ├── process-nadcp-1.jl            # NADCP-1 (2020/2021) processing
│       ├── process-nadcp-2.jl            # NADCP-2 (2021/2022) processing
│       ├── process-2019-state-files.jl   # 2019 state data processing
│       ├── process-2020-organized-farms.jl # 2020 organized farms
│       ├── process-2021-organized-farms.jl # 2021 organized farms
│       ├── process-2022-nadcp-3.jl       # NADCP-3 (2022) processing
│       └── process-2022-organized-farms.jl # 2022 organized farms
├── src/                                   # Core package source code
│   ├── FMDData.jl                        # Main module file
│   ├── consts.jl                         # Package constants and paths
│   ├── utils.jl                          # Utility functions
│   ├── error-handlers.jl                 # Error handling utilities
│   ├── precompilation.jl                 # Precompilation directives
│   ├── logfiles/                         # Source-level log files
│   ├── icar-cleaning/                    # Data cleaning functions (14 files)
│   │   ├── wrapper-functions.jl          # Main cleaning pipeline
│   │   ├── file-management.jl            # CSV I/O operations
│   │   ├── clean-column-names.jl         # Column name standardization
│   │   ├── state-checks.jl               # State name validation
│   │   ├── state-keys.jl                 # State reference dictionaries
│   │   ├── calculate-state-*.jl          # Count and seroprevalence calculations
│   │   ├── check-*.jl                    # Data validation functions
│   │   ├── total-row-functions.jl        # Totals processing
│   │   ├── select-calculated-columns.jl  # Column selection
│   │   └── sort-data.jl                  # Data sorting functions
│   └── icar-processing/                  # Data processing functions (3 files)
│       ├── data-inference.jl             # Missing value inference
│       ├── dataframe-operations.jl       # DataFrame combination utilities
│       └── metadata-operations.jl        # Metadata addition functions
├── test/                                  # Test suite
│   ├── runtests.jl                       # Main test runner
│   ├── integration-pipeline-test.jl      # End-to-end pipeline tests
│   ├── edge-case-tests.jl               # Edge case validation
│   ├── utils.jl                          # Test utilities
│   ├── error-handlers.jl                # Error handling tests
│   ├── test-data/                        # Test datasets
│   ├── icar-cleaning/                    # Unit tests for cleaning functions
│   └── icar-processing/                  # Unit tests for processing functions
├── test-data/                            # Integration test datasets
│   └── logfiles/                         # Test execution logs
├── Project.toml                          # Julia package configuration
├── Manifest.toml                         # Dependency lock file
├── LocalPreferences.toml                 # Local package preferences
├── README.md                             # Project overview and setup
├── CLAUDE.md                             # AI assistant guidance
└── fmd_data_availability_summary.md      # Data availability documentation
```

## Directory Details

### Core Data Pipeline
- **`inputs/`**: Raw data files including 31 extracted CSV tables from ICAR annual reports (2015-2022) and DIVA database exports
- **`data/cleaned/`**: Quality-checked data with standardized formatting, state name validation, and structural corrections (28 files)
- **`data/processed/`**: Analysis-ready datasets with metadata, inferred values, and combined datasets for different analysis needs (39 files)

### Source Code Organization
- **`src/icar-cleaning/`**: 14 modules handling data validation, standardization, calculations, and quality checks
- **`src/icar-processing/`**: 3 modules for metadata addition, value inference, and dataset combination
- **`scripts/`**: Orchestration scripts coordinating the cleaning and processing pipelines, including 7 report-specific processing files

### Data Processing Workflow
1. **Cleaning**: `scripts/icar-cleaning.jl` → `data/cleaned/` (standardization, validation, calculations)
2. **Processing**: `scripts/icar-additional-processing.jl` → `data/processed/` (metadata, inference, combination)

### Quality Assurance
- **Logging**: Comprehensive logging at each stage with separate logfiles for cleaning and processing
- **Testing**: Unit tests for all functions plus integration tests for the complete pipeline
- **Error Handling**: Distinguishes between recoverable warnings and fatal errors that halt processing

Each directory contains additional documentation in `README.md` files with specific details about data formats, processing steps, and usage instructions.

## Downloading the processed files

You can either:

1) navigate to the processed files in GitHub and download the raw files, or;
2) you can click on the "Raw" button to open the file in your browser, and then copy the URL (it should look something like "https://raw.githubusercontent.com/arnold-c/FMDData/refs/heads/main/data/icar-seroprevalence/processed/nadcp_1.csv"), which you can then use in a script to download the file programmatically.
3) you can clone the repository, which includes the cleaned and processed data files

The advantage of method 2) is that if the data files change then the script should capture this, though you will want to ensure you have some sort check to notify you when this happens.

## Running the cleaning files

If you download processed files as above, you shouldn't need to run the steps yourself.
But if you would like to do so, you can follow the outlined in [the cleaning tutorial](./cleaning-process.md)
