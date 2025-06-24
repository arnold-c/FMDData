# Cleaning the ICAR seroprevalence tables

- Install Julia (>= v1.11)
    - Recommended using [juliaup](https://github.com/JuliaLang/juliaup)
- Clone this repository
- Open the julia REPL in the repository directory and activate the project (`julia --project=.`)
- Instantiate the project to download the required packages (`using Pkg; Pkg.instantiate()`)
- Run the cleaning file (`include("./scripts/icar-cleaning.jl")`)
    - This currently terminates early as a couple of the raw files have irredeemable errors that should not be skipped over. To work around this, you can send each individual line of code to a running julia session (either copy and pasting by hand, or using some built-in functionality of your editor of choice).
- Run the processing file (`include("./scripts/icar-additional-processing.jl")`)


