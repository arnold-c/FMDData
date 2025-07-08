# FMDData

This project contains the code and cleaned FMD data sets for India's FMD disease elimination project.

```@contents
Pages = [
    "common.md",
    "icar-cleaning.md",
    "icar-processing.md",
    "internal.md"
]
Depth = 2
```

## Structure

```bash
❯ tree -d
.
├── data
│   └── icar-seroprevalence
│       ├── cleaned
│       │   └── logfiles
│       └── processed
│           └── logfiles
├── docs
├── inputs
│   └── ICAR-Reports
│       ├── extracted-seroprevalence-tables
│       │   └── README.md
│       ├── pdfs
│       └── README.md
├── scripts
├── src
└── test

19 directories
```

- `inputs/` contains all raw input files for cleaning and processing
    - `ICAR-Reports`:
        - `pdfs/` contains the PDF files for the ICAR reports
        - `extracted-seroprevalence-tables/` contains CSV files of the seroprevalence tables extracted from the PDF reports
- `src/` contains the functions required to clean and process the raw tables in `extracted-seroprevalence-tables/`
- `scripts/` contains the scripts that clean and process the raw tables in `extracted-seroprevalence-tables/`
- `data/` contains the cleaned and processed data files in CSV format
    - `cleaned/` contains the cleaned files (data quality checks and standardized formatting)
    - `processed/` contains the output of the processing of the `cleaned/` files i.e., adding relevant metadata such as the test type and threshold used, and inferring the sample year
        - `WARNINGS` are treated as notifications of errors found in the cleaning and processing of the data that can be handled e.g., use calculated values instead of those provided in the data when the provided values are likely not computed correctly
        - `ERRORS` are errors that cannot be handled and interrupt computation and will not produce an output file for further processing e.g., a state name that cannot be matched against the known values in `src/state-keys.jl`
- `test/` contains unit and integration tests for the cleaning and processing functions and steps

Each `README.md` contains additional details relating to their respective directories.

## Downloading the processed files

You can either:

1) navigate to the processed files in GitHub and download the raw files, or;
2) you can click on the "Raw" button to open the file in your browser, and then copy the URL (it should look something like "https://raw.githubusercontent.com/arnold-c/FMDData/refs/heads/main/data/icar-seroprevalence/processed/nadcp_1.csv"), which you can then use in a script to download the file programmatically.
3) you can clone the repository, which includes the cleaned and processed data files

The advantage of method 2) is that if the data files change then the script should capture this, though you will want to ensure you have some sort check to notify you when this happens.

## Running the cleaning files

If you download processed files as above, you shouldn't need to run the steps yourself.
But if you would like to do so, you can follow the outlined in [the cleaning tutorial](./cleaning-process.md)
