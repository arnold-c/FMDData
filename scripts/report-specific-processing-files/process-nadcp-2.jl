# Process NADCP-2
logfile = joinpath(icar_processed_logdir, "nadcp_2.log")
logger = FileLogger(logfile)

cumulative_nadcp_2_2022 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2022_Annual-Report_NADCP-2.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_2_2021 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2021_Annual-Report_NADCP-2.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_2_2022 = Try.@? FMDData._log_try_error(
    infer_later_year_values(
        cumulative_nadcp_2_2022,
        nadcp_2_2021,
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        cumulative_nadcp_2_2022 => OrderedDict(
            :sample_year => "2021/2022",
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_2_2022 => OrderedDict(
            :sample_year => 2022,
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_2_2021 => OrderedDict(
            :sample_year => 2021,
            :report_year => 2021,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

nadcp_2 = Try.@? FMDData._log_try_error(
    combine_round_dfs(
        cumulative_nadcp_2_2022, nadcp_2_2022, nadcp_2_2021
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("nadcp_2_2022.csv", icar_processed_dir(), nadcp_2_2022);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_2_2021.csv", icar_processed_dir(), nadcp_2_2021);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_2.csv", icar_processed_dir(), nadcp_2);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end