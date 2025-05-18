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
[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://arnold-c.github.io/FMDData.jl/stable)
[![Documentation](https://img.shields.io/badge/docs-master-blue.svg)](https://arnold-c.github.io/FMDData.jl/dev)
-->
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
