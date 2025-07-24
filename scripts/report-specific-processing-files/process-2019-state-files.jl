# Process all 2019 state files
# Consolidated processing for all 23 state files from 2019 ICAR reports

# Mapping from cleaned file state names to output file names
state_mapping = [
    ("A-N-Islands", "a_n_islands"),
    ("Andhra-Pradesh", "andhra_pradesh"),
    ("Bihar", "bihar"),
    ("Chhattisgarh", "chhattisgarh"),
    ("Goa", "goa"),
    ("Gujarat", "gujarat"),
    ("Haryana", "haryana"),
    ("Jammu-Kashmir", "jammu_kashmir"),
    ("Karnataka", "karnataka"),
    ("Kerala", "kerala"),
    ("Madhya-Pradesh", "madhya_pradesh"),
    ("Maharashtra", "maharashtra"),
    ("Manipur", "manipur"),
    ("Mizoram", "mizoram"),
    ("Odisha", "odisha"),
    ("Pondicherry", "pondicherry"),
    ("Punjab", "punjab"),
    ("Rajasthan", "rajasthan"),
    ("Tamil-Nadu", "tamil_nadu"),
    ("Telangana", "telangana"),
    ("Uttar-Pradesh", "uttar_pradesh"),
    ("Uttarakhand", "uttarakhand"),
    ("West-Bengal", "west_bengal"),
]

# Process each state file
for (input_state_name, output_state_name) in state_mapping
    println("Processing 2019 $(input_state_name)...")

    # Set up logging
    local logfile = joinpath(icar_processed_logdir, "$(output_state_name)_2019.log")
    local logger = FileLogger(logfile)

    # Load the cleaned CSV file
    state_data = Try.@? FMDData._log_try_error(
        load_csv(
            "clean_2019_Annual-Report_$(input_state_name).csv",
            icar_cleaned_dir()
        );
        logger = logger
    )

    # Add metadata (same for all 2019 state files)
    Try.@? FMDData._log_try_error(
        add_all_metadata!(
            state_data => OrderedDict(
                :report_year => 2019,
                :test_type => "SPCE",
                :test_threshold => "1.8 log10 @ 50% inhibition"
            )
        );
        logger = logger
    )

    # Rename columns (same for all 2019 state files)
    DataFrames.rename!(
        state_data,
        Dict(:year => "sample_year", :round => "round_name")
    )

    # Write processed file
    Try.@? FMDData._log_try_error(
        write_csv("$(output_state_name)_2019.csv", icar_processed_dir(), state_data);
        logger = logger
    )

    # Clean up empty log files
    if filesize(logfile) == 0
        rm(logfile)
    end
end

println("Completed processing all 23 2019 state files.")

