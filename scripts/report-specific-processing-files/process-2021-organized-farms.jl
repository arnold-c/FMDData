# Process 2021 Organized-farms
logfile = joinpath(icar_processed_logdir, "2021_organized_farms.log")
logger = FileLogger(logfile)

organized_farms_2021 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2021_Annual-Report_Organized-farms.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        organized_farms_2021 => OrderedDict(
            :sample_year => 2021,
            :report_year => 2021,
            :round_name => "Organized farms",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("organized_farms_2021.csv", icar_processed_dir(), organized_farms_2021);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end