# FMDData.jl

<!-- Tidyverse lifecycle badges, see https://www.tidyverse.org/lifecycle/ Uncomment or delete as needed. -->
![lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)<!--
![lifecycle](https://img.shields.io/badge/lifecycle-maturing-blue.svg)
![lifecycle](https://img.shields.io/badge/lifecycle-stable-green.svg)
![lifecycle](https://img.shields.io/badge/lifecycle-retired-orange.svg)
![lifecycle](https://img.shields.io/badge/lifecycle-archived-red.svg)
![lifecycle](https://img.shields.io/badge/lifecycle-dormant-blue.svg) -->
[![build](https://github.com/arnold-c/FMDData.jl/workflows/CI/badge.svg)](https://github.com/arnold-c/FMDData.jl/actions?query=workflow%3ACI)
<!-- travis-ci.com badge, uncomment or delete as needed, depending on whether you are using that service. -->
<!-- [![Build Status](https://travis-ci.com/arnold-c/FMDData.jl.svg?branch=master)](https://travis-ci.com/arnold-c/FMDData.jl) -->
<!-- NOTE: Codecov.io badge now depends on the token, copy from their site after setting up -->
<!-- Documentation -- uncomment or delete as needed -->
<!--

[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://arnold-c.github.io/FMDData.jl/dev)
-->
[![Documentation](https://img.shields.io/badge/docs-dev-blue.svg)](https://fmddata.callumarnold.com/dev)
[![Aqua QA](https://raw.githubusercontent.com/JuliaTesting/Aqua.jl/master/badge.svg)](https://github.com/JuliaTesting/Aqua.jl)
[![Tested with JET.jl](https://img.shields.io/badge/%F0%9F%9B%A9%EF%B8%8F_tested_with-JET.jl-233f9a)](https://github.com/aviatesk/JET.jl)
[![code style: runic](https://img.shields.io/badge/code_style-%E1%9A%B1%E1%9A%A2%E1%9A%BE%E1%9B%81%E1%9A%B2-black)](https://github.com/fredrikekre/Runic.jl)

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

If you download raw files as above, you shouldn't need to run the steps yourself.
But if you would like to do so, you can follow the steps below.

- Install Julia (>= v1.11)
    - Recommended using [juliaup](https://github.com/JuliaLang/juliaup)
- Clone this repository
- Open the julia REPL in the repository directory and activate the project (`julia --project=.`)
- Instantiate the project to download the required packages (`using Pkg; Pkg.instantiate()`)
- Run the cleaning file (`include("./scripts/icar-cleaning.jl")`)
    - This currently terminates early as a couple of the raw files have irredeemable errors that should not be skipped over. To work around this, you can send each individual line of code to a running julia session (either copy and pasting by hand, or using some built-in functionality of your editor of choice).
- Run the processing file (`include("./scripts/icar-additional-processing.jl")`)
