# Process 2022 Organized-farms
logfile = joinpath(icar_processed_logdir, "2022_organized_farms.log")
logger = FileLogger(logfile)

organized_farms_2022 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2022_Annual-Report_Organized-farms.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        organized_farms_2022 => OrderedDict(
            :sample_year => 2022,
            :report_year => 2022,
            :round_name => "Organized farms",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("organized_farms_2022.csv", icar_processed_dir(), organized_farms_2022);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end