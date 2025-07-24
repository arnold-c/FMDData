# Process NADCP-1
logfile = joinpath(icar_processed_logdir, "nadcp_1.log")
logger = FileLogger(logfile)

cumulative_nadcp_1_2021 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2021_Annual-Report_NADCP-1.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_1_2020 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2020_Annual-Report_NADCP-1.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_1_2021 = Try.@? FMDData._log_try_error(
    infer_later_year_values(
        cumulative_nadcp_1_2021,
        nadcp_1_2020,
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        cumulative_nadcp_1_2021 => OrderedDict(
            :sample_year => "2020/2021",
            :report_year => 2021,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_1_2021 => OrderedDict(
            :sample_year => 2021,
            :report_year => 2021,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_1_2020 => OrderedDict(
            :sample_year => 2020,
            :report_year => 2020,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

nadcp_1 = Try.@? FMDData._log_try_error(
    combine_round_dfs(
        cumulative_nadcp_1_2021, nadcp_1_2021, nadcp_1_2020
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("nadcp_1_2021.csv", icar_processed_dir(), nadcp_1_2021);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_1_2020.csv", icar_processed_dir(), nadcp_1_2020);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_1.csv", icar_processed_dir(), nadcp_1);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end