# Process 2020 Organized-farms
logfile = joinpath(icar_processed_logdir, "2020_organized_farms.log")
logger = FileLogger(logfile)

organized_farms_2020 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2020_Annual-Report_Organized-farms.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        organized_farms_2020 => OrderedDict(
            :sample_year => 2020,
            :report_year => 2020,
            :round_name => "Organized farms",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("organized_farms_2020.csv", icar_processed_dir(), organized_farms_2020);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end