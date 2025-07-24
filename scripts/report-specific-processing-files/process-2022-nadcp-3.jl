# Process 2022 NADCP-3
logfile = joinpath(icar_processed_logdir, "2022_nadcp_3.log")
logger = FileLogger(logfile)

nadcp_3_2022 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2022_Annual-Report_NADCP-3.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_3_2022 => OrderedDict(
            :sample_year => 2022,
            :report_year => 2022,
            :round_name => "NADCP 3",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("nadcp_3_2022.csv", icar_processed_dir(), nadcp_3_2022);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end